from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import os
import secrets

security = HTTPBasic()

API_ADMIN_USER = os.getenv("API_ADMIN_USER", "admin")
API_ADMIN_PASSWORD = os.getenv("API_ADMIN_PASSWORD", "admin")

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    is_correct_username = secrets.compare_digest(
        credentials.username.encode("utf8"),
        API_ADMIN_USER.encode("utf8")
    )
    is_correct_password = secrets.compare_digest(
        credentials.password.encode("utf8"),
        API_ADMIN_PASSWORD.encode("utf8")
    )
    
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return credentials.username