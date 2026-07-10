import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_agent_routing_and_reranking(monkeypatch):
    """Test agent routing, document reranking, and response generation.

    Args:
        monkeypatch (pytest.MonkeyPatch): Fixture for patching external dependencies.

    Asserts:
        The agent returns the expected response text, does not use cache,
        reranks source documents correctly, and creates a chat once.
    """
    mock_pinecone = MagicMock(
        return_value={
            "matches": [
                {
                    "id": "doc_1",
                    "metadata": {
                        "text": "General market noise.",
                        "sentiment": "NEUTRAL",
                    },
                },
                {
                    "id": "doc_2",
                    "metadata": {
                        "text": "Crucial news about Apple.",
                        "sentiment": "POSITIVE",
                    },
                },
            ]
        }
    )
    monkeypatch.setattr("app.routers.agent.index.query", mock_pinecone)

    mock_rerank = MagicMock(return_value=[-2.0, 5.0])
    monkeypatch.setattr("app.routers.agent.reranker.predict", mock_rerank)

    monkeypatch.setattr("app.routers.agent.cache.save", MagicMock())

    # Gemini SDK RAM instantiation FIX.
    fake_response = MagicMock()
    fake_response.text = "Apple's new product looks promising based on the news."
    fake_chat = AsyncMock()
    fake_chat.send_message.return_value = fake_response
    fake_chats_manager = MagicMock()
    fake_chats_manager.create.return_value = fake_chat
    fake_aio = MagicMock()
    fake_aio.chats = fake_chats_manager
    fake_client = MagicMock()
    fake_client.aio = fake_aio

    monkeypatch.setattr("app.routers.agent.client", fake_client)

    from app.routers.agent import run_agent_with_history

    response_text, sources, is_cached, model_used = await run_agent_with_history(
        query="What is happening with Apple?",
        message_history=[],
        user_id="user_test_123",
        tenant_id="tenant_alpha",
    )

    assert response_text == "Apple's new product looks promising based on the news."
    assert is_cached is False
    assert len(sources) == 2
    assert sources[0]["text"] == "Crucial news about Apple."
    assert sources[0]["rerank_score"] == 5.0
    fake_chats_manager.create.assert_called_once()
