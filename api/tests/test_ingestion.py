import pytest
from unittest.mock import patch, MagicMock


@patch("app.routers.ingestion.s3_client")
@patch("app.routers.ingestion.pika.BlockingConnection")
def test_ingest_news_happy_path(mock_pika, mock_s3, client):
    """Test that news ingestion queues the payload and stores it in S3.

    Args:
        mock_pika: mock for pika.BlockingConnection.
        mock_s3: mock for the S3 client.
        client: test client fixture.
    """

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_pika.return_value = mock_connection
    mock_s3.head_bucket.return_value = True

    payload = {
        "text": "TITLE: Fed raises interest rates. DESCRIPTION: The federal reserve announced a 50 bps hike...",
        "published_at": 1720447200.0,
        "url": "https://ft.com/test",
        "tickers": ["SPY", "TLT"],
    }

    response = client.post("/api/v1/ingest/", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "queued"}
    assert mock_s3.put_object.called
    assert mock_channel.basic_publish.called


def test_ingest_news_empty_text_validation(client):
    (
        """_summary_

    Args:
        client (_type_): _description_
    """
        """Prueba que el endpoint rechace una noticia si el texto está vacío y devuelva el 422 seguro."""
    )

    payload = {
        "text": "   ",
        "published_at": 1720447200.0,
        "url": "https://ft.com/test",
        "tickers": ["SPY"],
    }

    response = client.post("/api/v1/ingest/", json=payload)

    assert response.status_code == 422
