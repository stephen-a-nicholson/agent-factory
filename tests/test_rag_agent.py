from types import SimpleNamespace
from unittest.mock import MagicMock

from src.agent.rag_agent import (
    GENIE_TOOL_NAME,
    RETRIEVER_TOOL_NAME,
    RagAgent,
    build_tool_specs,
    extract_genie_answer_text,
    extract_text_content,
    format_retrieved_chunks,
    parse_tool_call_arguments,
    rows_from_search_result,
)


def test_build_tool_specs_without_genie():
    specs = build_tool_specs([], genie_enabled=False)
    assert [s["function"]["name"] for s in specs] == [RETRIEVER_TOOL_NAME]
    assert specs[0]["function"]["parameters"]["required"] == ["query"]


def test_build_tool_specs_with_filters_and_genie():
    specs = build_tool_specs(["category"], genie_enabled=True)
    names = [s["function"]["name"] for s in specs]
    assert names == [RETRIEVER_TOOL_NAME, GENIE_TOOL_NAME]
    retriever_props = specs[0]["function"]["parameters"]["properties"]
    assert "category" in retriever_props
    assert "query" in retriever_props


def test_parse_tool_call_arguments_valid_json():
    assert parse_tool_call_arguments('{"query": "cost cap"}') == {"query": "cost cap"}


def test_parse_tool_call_arguments_invalid_json_returns_empty():
    assert parse_tool_call_arguments("not json") == {}


def test_parse_tool_call_arguments_non_dict_returns_empty():
    assert parse_tool_call_arguments("[1, 2, 3]") == {}


def test_format_retrieved_chunks_empty():
    assert format_retrieved_chunks([]) == "No matching passages were found."


def test_format_retrieved_chunks_includes_title_category_and_content():
    rows = [
        {
            "title": "2026 Sporting Regulations",
            "category": "sporting",
            "content": "Article B1.4: Insurance requirements.",
            "source_url": "https://example.com/sporting.pdf",
        }
    ]
    formatted = format_retrieved_chunks(rows)
    assert "2026 Sporting Regulations" in formatted
    assert "sporting" in formatted
    assert "Article B1.4" in formatted
    assert "https://example.com/sporting.pdf" in formatted


def test_extract_text_content_plain_string():
    assert extract_text_content("hello") == "hello"


def test_extract_text_content_reasoning_model_blocks():
    # Confirmed against a real databricks-gpt-oss-120b response: content
    # can be a list of typed blocks (reasoning + text), not a plain string.
    content = [
        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "thinking..."}]},
        {"type": "text", "text": "The answer is 42."},
    ]
    assert extract_text_content(content) == "The answer is 42."


def test_extract_text_content_none():
    assert extract_text_content(None) == ""


def test_extract_genie_answer_text():
    message = SimpleNamespace(
        attachments=[SimpleNamespace(text=SimpleNamespace(content="Stroll, 30 retirements"))]
    )
    assert extract_genie_answer_text(message) == "Stroll, 30 retirements"


def test_extract_genie_answer_text_no_attachments():
    assert extract_genie_answer_text(SimpleNamespace(attachments=[])) == ""


def test_run_tool_routes_retriever_and_pops_query():
    agent = RagAgent()
    agent._call_retriever = MagicMock(return_value="retrieved text")

    result = agent._run_tool(RETRIEVER_TOOL_NAME, {"query": "cost cap", "category": "sporting"})

    assert result == "retrieved text"
    agent._call_retriever.assert_called_once_with("cost cap", category="sporting")


def test_run_tool_routes_genie():
    agent = RagAgent()
    agent._call_genie = MagicMock(return_value="genie answer")

    result = agent._run_tool(GENIE_TOOL_NAME, {"question": "who won?"})

    assert result == "genie answer"
    agent._call_genie.assert_called_once_with("who won?")


def test_run_tool_unknown_name():
    agent = RagAgent()
    assert agent._run_tool("nonsense", {}) == "Unknown tool: nonsense"


def test_call_genie_without_space_id_configured():
    agent = RagAgent()
    agent.config = {}
    assert agent._call_genie("who won?") == (
        "The structured-data tool is not configured for this agent."
    )


def test_rows_from_search_result():
    result = {
        "result": {
            "data_array": [
                ["Title A", "sporting", "content a", "https://a"],
                ["Title B", "technical", "content b", "https://b"],
            ]
        }
    }
    rows = rows_from_search_result(result)
    assert rows == [
        {
            "title": "Title A",
            "category": "sporting",
            "content": "content a",
            "source_url": "https://a",
        },
        {
            "title": "Title B",
            "category": "technical",
            "content": "content b",
            "source_url": "https://b",
        },
    ]


def test_rows_from_search_result_empty():
    assert rows_from_search_result({"result": {"data_array": []}}) == []


def test_call_retriever_retries_without_filters_when_filtered_search_empty():
    agent = RagAgent()
    index = MagicMock()
    agent._vector_search_index = MagicMock(return_value=index)
    # First call (filtered) returns nothing; second (unfiltered) succeeds.
    index.similarity_search.side_effect = [
        {"result": {"data_array": []}},
        {"result": {"data_array": [["Title A", "sporting", "content a", "https://a"]]}},
    ]

    result = agent._call_retriever("cost cap", category="sporting regulations")

    assert "content a" in result
    assert index.similarity_search.call_count == 2
    first_call, second_call = index.similarity_search.call_args_list
    assert first_call.kwargs["filters"] == {"category": "sporting regulations"}
    assert second_call.kwargs["filters"] is None


def test_call_retriever_does_not_retry_when_no_filters_given():
    agent = RagAgent()
    index = MagicMock()
    agent._vector_search_index = MagicMock(return_value=index)
    index.similarity_search.return_value = {"result": {"data_array": []}}

    result = agent._call_retriever("cost cap")

    assert result == "No matching passages were found."
    assert index.similarity_search.call_count == 1


def test_call_retriever_does_not_retry_when_filtered_search_succeeds():
    agent = RagAgent()
    index = MagicMock()
    agent._vector_search_index = MagicMock(return_value=index)
    index.similarity_search.return_value = {
        "result": {"data_array": [["Title A", "sporting", "content a", "https://a"]]}
    }

    agent._call_retriever("cost cap", category="sporting")

    assert index.similarity_search.call_count == 1
