from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request


def get_user_or_ip(request: Request) -> str:
    """Return an identifier for rate limiting.

    If the request includes an Authorization header, use that header value as
    the key. Otherwise, fall back to the remote client IP address.

    Args:
        request (Request): Incoming FastAPI request.

    Returns:
        str: A string identifying the user or client IP for rate limiting.
    """
    auth_header = request.headers.get("Authorization")
    if auth_header:
        return auth_header

    return get_remote_address(request)


limiter = Limiter(key_func=get_user_or_ip)
