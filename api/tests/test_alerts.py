def test_create_alert_happy_path(client):
    """Test successful creation of a price alert with valid payload.

    Args:
        client: Test client for making HTTP requests.
    """

    payload = {"ticker": "AAPL", "target_price": 150.50, "condition": "above"}

    response = client.post("/api/v1/alerts/", json=payload)

    assert response.status_code == 200

    data = response.json()
    assert data["message"] == "Alert created successfully."
    assert data["ticker"] == "AAPL"


def test_create_alert_sad_path_negative_price(client):
    """Verify that the API rejects creating an alert
    when the target price is negative.

    Args:
        client: Test client for making HTTP requests.
    """

    payload = {
        "ticker": "MSFT",
        "target_price": -20.0,
        "condition": "below",
    }

    response = client.post("/api/v1/alerts/", json=payload)

    assert response.status_code == 422
    assert "target_price" in response.text


def test_create_alert_sad_path_bad_condition(client):
    """Test that invalid condition values are rejected with validation error.

    Args:
        client: Test client for making HTTP requests.
    """

    payload = {
        "ticker": "TSLA",
        "target_price": 200.0,
        "condition": "equal",
    }

    response = client.post("/api/v1/alerts/", json=payload)
    assert response.status_code == 422


def test_get_alerts_isolation(client):
    """Verify alerts are returned independently and no cross-test state remains.

    Args:
        client: Test client for making HTTP requests.
    """

    client.post(
        "/api/v1/alerts/",
        json={"ticker": "GOOGL", "target_price": 100.0, "condition": "above"},
    )

    response = client.get("/api/v1/alerts/")

    assert response.status_code == 200
    data = response.json()

    assert len(data) == 1
    assert data[0]["ticker"] == "GOOGL"
    assert data[0]["target_price"] == 100.0
    assert data[0]["status"] == "active"
