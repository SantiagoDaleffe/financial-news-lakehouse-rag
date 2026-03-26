import jwt
import time

SECRET = "clave-secreta-de-prueba-para-local"

# Token del dueño real de la alerta
token_dueno = jwt.encode({"sub": "default_user", "exp": int(time.time()) + 3600}, SECRET, algorithm="HS256")

# Token del intruso
token_hacker = jwt.encode({"sub": "hacker_malo", "exp": int(time.time()) + 3600}, SECRET, algorithm="HS256")

print(f"TOKEN DUEÑO:\n{token_dueno}\n")
print(f"TOKEN HACKER:\n{token_hacker}")