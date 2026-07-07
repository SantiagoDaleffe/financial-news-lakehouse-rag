def test_create_alert_happy_path(client):
    """Prueba que un payload correcto cree una alerta exitosamente."""
    
    payload = {
        "ticker": "AAPL",
        "target_price": 150.50,
        "condition": "above"
    }
    
    # Simulamos una petición POST real
    response = client.post("/api/v1/alerts/", json=payload)
    
    # 1. Validamos que el código HTTP sea exitoso
    assert response.status_code == 200
    
    # 2. Validamos que el JSON de respuesta tenga lo que esperamos
    data = response.json()
    assert data["message"] == "Alert created successfully."
    assert data["ticker"] == "AAPL"

def test_create_alert_sad_path_negative_price(client):
    """Prueba que Pydantic bloquee precios negativos."""
    
    payload = {
        "ticker": "MSFT",
        "target_price": -20.0, # Precio inválido
        "condition": "below"
    }
    
    response = client.post("/api/v1/alerts/", json=payload)
    
    # Tiene que rebotar con 422 Unprocessable Entity (Validación fallida)
    assert response.status_code == 422
    assert "target_price" in response.text # El error tiene que mencionar el campo fallido

def test_create_alert_sad_path_bad_condition(client):
    """Prueba que Pydantic bloquee condiciones que no sean 'above' o 'below'."""
    
    payload = {
        "ticker": "TSLA",
        "target_price": 200.0,
        "condition": "equal" # Condición inválida según el Regex de Pydantic
    }
    
    response = client.post("/api/v1/alerts/", json=payload)
    assert response.status_code == 422

def test_get_alerts_isolation(client):
    """Prueba que el GET traiga las alertas guardadas para este usuario."""
    
    # Primero insertamos una alerta
    client.post("/api/v1/alerts/", json={
        "ticker": "GOOGL",
        "target_price": 100.0,
        "condition": "above"
    })
    
    # Luego hacemos un GET para ver si se guardó
    response = client.get("/api/v1/alerts/")
    
    assert response.status_code == 200
    data = response.json()
    
    # Debería haber exactamente 1 alerta, porque la base de datos 
    # se borra y recrea en cada test individual (gracias al conftest.py)
    assert len(data) == 1
    assert data[0]["ticker"] == "GOOGL"
    assert data[0]["target_price"] == 100.0
    assert data[0]["status"] == "active"