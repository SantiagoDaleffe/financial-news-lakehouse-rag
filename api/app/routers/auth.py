from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from ..security import supabase
from ..schemas import Token

router = APIRouter(tags=["auth"])


@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Authenticate user with email and password.

    Args:
        form_data (OAuth2PasswordRequestForm, optional): OAuth2 form with username and password. Defaults to Depends().

    Raises:
        HTTPException: 500 error if Supabase is not configured.
        HTTPException: 401 error if email or password is incorrect.

    Returns:
        dict: Access token and token type for authenticated user.
    """
    if not supabase:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Supabase is not configured.",
        )
    try:
        res = supabase.auth.sign_in_with_password(
            {"email": form_data.username, "password": form_data.password}
        )
        return {"access_token": res.session.access_token, "token_type": "bearer"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password.",
        )
