from unittest.mock import MagicMock

from src.agent.promote import promote


def test_promote_moves_champion_alias_to_candidate_version():
    client = MagicMock()
    client.get_model_version_by_alias.return_value = MagicMock(version="4")

    version = promote(client, "cat.schema.rag_agent")

    assert version == "4"
    client.get_model_version_by_alias.assert_called_once_with("cat.schema.rag_agent", "candidate")
    client.set_registered_model_alias.assert_called_once_with(
        "cat.schema.rag_agent", "champion", "4"
    )
