from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt
import os
from datetime import datetime, timedelta

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "secret_key")
ALGORITHM = "HS256"

router = APIRouter(tags=["auth"])

@router.post("/login")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Native endpoint for Swagger and the frontend to generate the JWT token.
    """
    if form_data.username == "admin" and form_data.password == "admin":
        payload = {
            "sub": "default_user",
            "exp": datetime.utcnow() + timedelta(days=1)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
        return {"access_token": token, "token_type": "bearer"}
        
    raise HTTPException(
        status_code=401,
        detail="Incorrect password or user",
        headers={"WWW-Authenticate": "Bearer"},
    )

def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    """Valida el token en cada petición protegida."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid Token")
        return str(user_id)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Expired Token")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid Token")