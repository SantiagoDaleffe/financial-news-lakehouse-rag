import pytest
from app.models import Conversation, Message


async def mock_run_agent(*args, **kwargs):
    """Simulate an agent generating a chatbot response.

    Args:
        *args: Positional arguments forwarded to the agent.
        **kwargs: Keyword arguments forwarded to the agent.

    Returns:
        tuple[str, list[dict[str, str]], bool, str]: Simulated response text,
            a list of source dictionaries, a completion flag, and the model name.
    """
    return "I am a simulated bot", [{"text": "source"}], False, "gemini-3.5-flash"


def test_chat_happy_path_new_conversation(client, monkeypatch):
    """_summary_

    Args:
        client (_type_): _description_
        monkeypatch (_type_): _description_
    """

    monkeypatch.setattr("app.routers.chats.run_agent_with_history", mock_run_agent)

    payload = {"message": "Hello market"}

    response = client.post("/api/v1/chats/", json=payload)

    assert response.status_code == 200
    data = response.json()

    assert data["response"] == "I am a simulated bot"
    assert "conversation_id" in data
    assert data["credits_remaining"] == 98.0
    assert data["model_used"] == "gemini-3.5-flash"


def test_chat_invalid_conversation_id(client):
    """_summary_

    Args:
        client (_type_): _description_
    """

    payload = {
        "message": "I want to resume a conversation that doesn't exist",
        "conversation_id": 999999,
    }

    response = client.post("/api/v1/chats/", json=payload)

    assert response.status_code == 404
    assert "Conversation not found" in response.text


def test_chat_empty_message_validation(client):
    """_summary_

    Args:
        client (_type_): _description_
    """

    payload = {"message": ""}

    response = client.post("/api/v1/chats/", json=payload)
    assert response.status_code == 422
