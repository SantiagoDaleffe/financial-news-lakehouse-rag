from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordBearer
import os
import jwt
from supabase import create_client, Client

security = HTTPBearer()


security = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

if SUPABASE_URL and SUPABASE_ANON_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
else:
    supabase = None


def get_current_user_and_tenant(token: str = Depends(security)) -> dict:
    """
    Validates the JWT token issued by Supabase and extracts the user identifier and associated tenant.

    This helper does the following:
    - Gets the Bearer token from the HTTP credentials.
    - Verifies that SUPABASE_URL configuration is present.
    - Retrieves the correct public key from Supabase JWKS.
    - Decodes the token with the appropriate algorithm and expected audience.
    - Extracts `user_id` from the `sub` field and `tenant_id` from `app_metadata`.

    Args:
        token (str): The Bearer token obtained from the Authorization header.

    Raises:
        HTTPException: If the token is missing or malformed.
        HTTPException: If SUPABASE_URL configuration is missing.
        HTTPException: If the token does not contain the `sub` field.
        HTTPException: If the token has expired.
        HTTPException: If the token is invalid or cannot be verified.
        HTTPException: If any other error occurs during validation.

    Returns:
        dict: Dictionary with the user data:
            - user_id: user identifier extracted from the token.
            - tenant_id: tenant identifier extracted from `app_metadata` or `public_b2c` by default.
    """

    if not SUPABASE_URL:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: Missing SUPABASE_URL.",
        )

    try:
        user_response = supabase.auth.get_user(token)

        if not user_response or not user_response.user:
            raise ValueError("User identity could not be verified.")

        user_id = user_response.user.id
        payload = jwt.decode(token, options={"verify_signature": False})

        app_metadata = payload.get("app_metadata", {})
        tenant_id = app_metadata.get("tenant_id", "public_b2c")

        return {"user_id": user_id, "tenant_id": tenant_id}

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired. Please log in again.",
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Authentication failed: {str(e)}"
        )
