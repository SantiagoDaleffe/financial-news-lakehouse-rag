from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
import os

security = HTTPBearer()

# Esta clave es vital. Tiene que ser LA MISMA que use el frontend (Supabase, Auth0, etc.)
# Nunca se hardcodea, siempre va al .env
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "clave-secreta-de-prueba-para-local")
ALGORITHM = "HS256"

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Valida el token JWT y devuelve el user_id. 
    Si el token es falso o expiró, corta la petición automáticamente.
    """
    token = credentials.credentials
    try:
        # Desencriptamos el token usando nuestra palabra secreta
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # 'sub' (subject) es el estándar internacional en JWT para el ID de usuario
        user_id = payload.get("sub") 
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token inválido: falta el usuario")
            
        return str(user_id)
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="El token expiró. Volvé a iniciar sesión.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Credenciales inválidas o token falsificado.")