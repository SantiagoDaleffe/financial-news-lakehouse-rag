import requests
import json

# Leemos el archivo de texto
with open('bitcoin_wiki.txt', 'r', encoding='utf-8') as f:
    texto_gigante = f.read()

url = "http://localhost:8000/ingest"
payload = {"text": texto_gigante}

print("Enviando documento a la API...")
# requests se encarga de formatear todo a JSON perfecto para que no se rompa
response = requests.post(url, json=payload)

print(f"Status: {response.status_code}")
print(f"Respuesta: {response.json()}")