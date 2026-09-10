"""RAG agent for a domain: retrieves regulation chunks via vector search,
optionally calls the domain's Genie space as a tool for structured-data
questions, and answers using a configured LLM serving endpoint.

Implements the MLflow ResponsesAgent interface (see docs/DESIGN.md
section 3), logged with src/agent/deploy.py using MLflow's "models from
code" pattern. This file must stay fully self-contained, with no imports
from the rest of this repo's src/ package: at serving time only this file
(not the whole repo checkout) is present in the model's container, unlike
a Databricks job, which runs against a synced copy of the whole repo. See
src/ingest/structured.py and src/ingest/documents.py for the equivalent
job-side constraint (__file__/sys.path), which does not apply here for
the same reason: this file never runs as a spark_python_task.

Configuration (LLM endpoint, vector index, Genie space, system prompt,
retriever settings) comes from MLflow's ModelConfig, populated at logging
time from domain.yml by deploy.py, not hardcoded here.

Pure helpers (build_tool_specs, parse_tool_call_arguments,
format_retrieved_chunks, extract_genie_answer_text, extract_text_content)
are unit-tested directly; the tool-calling loop itself needs live
LLM/vector-search/Genie endpoints and is exercised by actually deploying
and querying the agent, per docs/PLAN.md notes.
"""

from __future__ import annotations

import json
import time
from typing import Any
from uuid import uuid4

import mlflow
from mlflow.entities import SpanType
from mlflow.models import ModelConfig
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

RETRIEVER_TOOL_NAME = "search_regulations"
GENIE_TOOL_NAME = "ask_structured_data"
MAX_TOOL_ITERATIONS = 6
RETRIEVAL_COLUMNS = ["title", "category", "content", "source_url"]


def build_tool_specs(filter_columns: list[str], genie_enabled: bool) -> list[dict[str, Any]]:
    retriever_properties: dict[str, Any] = {
        "query": {"type": "string", "description": "The search query."}
    }
    for column in filter_columns:
        retriever_properties[column] = {
            "type": "string",
            "description": f"Optional exact-match filter on {column}.",
        }
    specs = [
        {
            "type": "function",
            "function": {
                "name": RETRIEVER_TOOL_NAME,
                "description": (
                    "Search the regulation documents for passages relevant to a question."
                ),
                "parameters": {
                    "type": "object",
                    "properties": retriever_properties,
                    "required": ["query"],
                },
            },
        }
    ]
    if genie_enabled:
        specs.append(
            {
                "type": "function",
                "function": {
                    "name": GENIE_TOOL_NAME,
                    "description": (
                        "Ask a question about structured results data: standings, points, "
                        "retirements or pit stop statistics. Do not use this for questions "
                        "about regulations, and do not invent statistics yourself."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {"question": {"type": "string"}},
                        "required": ["question"],
                    },
                },
            }
        )
    return specs


def parse_tool_call_arguments(raw_arguments: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_arguments)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def rows_from_search_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    data = result.get("result", {}).get("data_array", [])
    return [dict(zip(RETRIEVAL_COLUMNS, row, strict=False)) for row in data]


def format_retrieved_chunks(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No matching passages were found."
    parts = []
    for row in rows:
        title = row.get("title") or "unknown document"
        category = row.get("category") or "unknown category"
        source_url = row.get("source_url") or ""
        parts.append(f"[{title} ({category})] {source_url}\n{row.get('content', '')}")
    return "\n\n".join(parts)


def extract_genie_answer_text(message: Any) -> str:
    """Pulls the text answer out of a databricks-sdk GenieMessage, the
    same attachments-list shape used in src/evaluate/genie.py (duplicated
    rather than imported: see this file's module docstring)."""
    for attachment in getattr(message, "attachments", None) or []:
        text = getattr(attachment, "text", None)
        if text is not None:
            return text.content or ""
    return ""


def extract_text_content(content: Any) -> str:
    """The LLM response's message content is usually a plain string, but
    reasoning models return a list of typed blocks instead (confirmed
    against a real databricks-gpt-oss-120b response while building this
    agent); pull just the answer text out either way."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    return ""


class RagAgent(ResponsesAgent):
    def __init__(self) -> None:
        # At serving time, MLflow substitutes the real config passed to
        # log_model(model_config=...) here, regardless of
        # development_config. Outside that context (importing this file
        # directly, e.g. for pytest to reach the pure helper functions
        # below), ModelConfig raises FileNotFoundError on an empty dict,
        # so development_config needs a placeholder key even though
        # nothing meaningful can be defaulted locally.
        self.config = ModelConfig(development_config={"_unconfigured": True}).to_dict()

    def _deploy_client(self):
        from mlflow.deployments import get_deploy_client

        return get_deploy_client("databricks")

    def _vector_search_index(self):
        from databricks.ai_search.client import AISearchClient

        client = AISearchClient(disable_notice=True)
        return client.get_index(index_name=self.config["vector_search_index"])

    def _workspace_client(self):
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()

    def _similarity_search(
        self, index, query: str, vs_filters: dict[str, str] | None
    ) -> list[dict[str, Any]]:
        result = index.similarity_search(
            query_text=query,
            columns=RETRIEVAL_COLUMNS,
            num_results=self.config.get("top_k", 6),
            filters=vs_filters,
        )
        return rows_from_search_result(result)

    @mlflow.trace(span_type=SpanType.RETRIEVER)
    def _call_retriever(self, query: str, **filters: str) -> list[dict[str, Any]]:
        """Returns structured rows, not formatted text: mlflow.genai's
        RetrievalGroundedness/RetrievalRelevance/RetrievalSufficiency
        scorers extract retrieval context straight from this RETRIEVER
        span's captured return value, and only recognise a list of dicts
        with a page_content/content/text key, not a pre-joined string.
        Confirmed by a real evaluate_<name> run: with this returning a
        formatted string, mlflow.genai.evaluate()'s result.metrics had no
        retrieval_groundedness/mean key at all (not a low score, an absent
        one). _run_tool formats this into text for the tool-call response;
        the LLM never sees this method's return value directly. See
        docs/PLAN.md notes, phase 3."""
        index = self._vector_search_index()
        vs_filters = {k: v for k, v in filters.items() if v} or None

        rows = self._similarity_search(index, query, vs_filters)
        if vs_filters and not rows:
            # The LLM's filter value isn't guaranteed to match the
            # chunks' actual category values exactly (confirmed against a
            # real deploy: it filtered on "sporting regulations" when the
            # ingested chunks are tagged "sporting", got zero matches, and
            # went on to answer with a hallucinated article number instead
            # of admitting it hadn't retrieved anything). Retry unfiltered
            # rather than let a bad filter guess silently starve the
            # answer of any grounding at all.
            rows = self._similarity_search(index, query, None)
        return rows

    @mlflow.trace(span_type=SpanType.TOOL)
    def _call_genie(self, question: str) -> str:
        space_id = self.config.get("genie_space_id")
        if not space_id:
            return "The structured-data tool is not configured for this agent."
        client = self._workspace_client()
        # Not start_conversation_and_wait: its generic polling helper
        # raises on a failed message without surfacing GenieMessage.error,
        # just the bare status ("failed to reach COMPLETED, got
        # MessageStatus.FAILED"), which isn't enough to debug a real
        # failure by. Poll manually instead so a failure can still read
        # .error off the final message.
        from databricks.sdk.service.dashboards import MessageStatus

        waiter = client.genie.start_conversation(space_id, question)
        conversation_id = waiter.response.conversation_id
        message_id = waiter.response.message_id
        deadline = time.time() + 120
        message = None
        while time.time() < deadline:
            message = client.genie.get_message(
                conversation_id=conversation_id, message_id=message_id, space_id=space_id
            )
            if message.status in (MessageStatus.COMPLETED, MessageStatus.FAILED):
                break
            time.sleep(2)

        if message is None or message.status != MessageStatus.COMPLETED:
            error = getattr(message, "error", None) if message else None
            status = message.status if message else "timed out"
            return f"The structured-data tool failed (status: {status}, error: {error})."
        return extract_genie_answer_text(message) or "Genie did not return an answer."

    def _run_tool(self, name: str, arguments: dict[str, Any]) -> str:
        if name == RETRIEVER_TOOL_NAME:
            filters = dict(arguments)
            query = filters.pop("query", "")
            return format_retrieved_chunks(self._call_retriever(query, **filters))
        if name == GENIE_TOOL_NAME:
            return self._call_genie(arguments.get("question", ""))
        return f"Unknown tool: {name}"

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(self, request: ResponsesAgentRequest):
        messages = self.prep_msgs_for_cc_llm(request.input)
        system_prompt = self.config.get("system_prompt", "")
        if system_prompt and (not messages or messages[0].get("role") != "system"):
            messages = [{"role": "system", "content": system_prompt}, *messages]

        tools = build_tool_specs(
            self.config.get("filter_columns", []), bool(self.config.get("genie_space_id"))
        )
        client = self._deploy_client()
        llm_endpoint = self.config["llm_endpoint"]

        for _ in range(MAX_TOOL_ITERATIONS):
            response = client.predict(
                endpoint=llm_endpoint,
                inputs={"messages": messages, "tools": tools, "max_tokens": 1024},
            )
            message = response["choices"][0]["message"]
            messages.append(message)

            tool_calls = message.get("tool_calls") or []
            if not tool_calls:
                text = extract_text_content(message.get("content"))
                item = self.create_text_output_item(text=text, id=str(uuid4()))
                yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)
                return

            for tool_call in tool_calls:
                call_id = tool_call["id"]
                name = tool_call["function"]["name"]
                arguments = parse_tool_call_arguments(tool_call["function"]["arguments"])

                call_item = self.create_function_call_item(
                    id=str(uuid4()),
                    call_id=call_id,
                    name=name,
                    arguments=json.dumps(arguments),
                )
                yield ResponsesAgentStreamEvent(type="response.output_item.done", item=call_item)

                output = self._run_tool(name, arguments)
                messages.append({"role": "tool", "tool_call_id": call_id, "content": output})

                output_item = self.create_function_call_output_item(call_id=call_id, output=output)
                yield ResponsesAgentStreamEvent(type="response.output_item.done", item=output_item)

        item = self.create_text_output_item(
            text="I wasn't able to finish answering within the tool-call budget.",
            id=str(uuid4()),
        )
        yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)


mlflow.models.set_model(RagAgent())
