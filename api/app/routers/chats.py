from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session
from ..models import Conversation, Message, User
from ..schemas import ChatRequest   
from ..security import get_current_user_and_tenant
from .alerts import get_db
from .agent import run_agent_with_history
from ..schemas import ChatResponse

router = APIRouter(tags=["chat"])
limiter = Limiter(key_func=get_remote_address)

MODEL_COSTS = {
    "gemini-3.5-flash": 2.0,
    "gemini-3.1-pro-preview": 5.0,
    "gemini-2.5-pro": 3.0,
    "gemini-3.1-flash-lite": 0.5,
    "gemini-2.5-flash-lite": 0.5,
}


@router.post("/", response_model=ChatResponse)
@limiter.limit("5/minute")
async def chat(
    request: Request,
    chat_request: ChatRequest,
    auth_data: dict = Depends(get_current_user_and_tenant),
    db: Session = Depends(get_db),
):
    """Handle a chat request for the current authenticated user.

    This endpoint creates or resumes a conversation, stores the user message,
    runs the agent with conversation history, stores the AI response, and
    deducts the request cost from the user's credits.

    Args:
        request (Request): HTTP request object for rate limiting and context.
        chat_request (ChatRequest): Payload containing the user message,
            optional conversation_id, and optional model_override.
        auth_data (dict, optional): Authentication and tenant data from
            get_current_user_and_tenant dependency.
        db (Session, optional): Database session dependency.

    Raises:
        HTTPException: If the conversation is not found or access is denied.
        HTTPException: If the user has insufficient credits.

    Returns:
        dict: Response containing conversation_id, AI response,
            sources, cache status, model used, and remaining credits.
    """
    user_id = auth_data["user_id"]
    tenant_id = auth_data["tenant_id"]

    user = (
        db.query(User).filter(User.id == user_id, User.tenant_id == tenant_id).first()
    )

    # DEV
    if not user:
        user = User(
            id=user_id, tenant_id=tenant_id, email=f"{user_id}@test.com", credits=100.0
        )
        db.add(user)
        db.commit()
        db.refresh(user)

    if user.credits <= 0:
        raise HTTPException(
            status_code=402, detail="Insufficient tokens to perform the query."
        )

    if not chat_request.conversation_id:
        new_conversation = Conversation(
            user_id=user_id,
            tenant_id=tenant_id,
            title=chat_request.message[:40] + "...",
        )
        db.add(new_conversation)
        db.flush()
        conversation_id = new_conversation.id
    else:
        conversation_id = chat_request.conversation_id
        conversation = (
            db.query(Conversation)
            .filter(
                Conversation.id == conversation_id,
                Conversation.user_id == user_id,
                Conversation.tenant_id == tenant_id,
            )
            .first()
        )
        if not conversation:
            raise HTTPException(
                status_code=404, detail="Conversation not found or access denied."
            )

    new_message = Message(
        conversation_id=conversation_id,
        user_id=user_id,
        tenant_id=tenant_id,
        role="user",
        content=chat_request.message,
    )
    db.add(new_message)
    db.flush()

    message_history = (
        db.query(Message)
        .filter(
            Message.conversation_id == conversation_id, Message.id != new_message.id
        )
        .order_by(Message.created_at)
        .all()
    )

    ai_response, sources, is_cached, model_used = await run_agent_with_history(
        chat_request.message,
        message_history,
        user_id,
        tenant_id,
        chat_request.model_override,
    )

    cost = 0.0
    if not is_cached and model_used != "failed_all":
        cost = MODEL_COSTS.get(model_used, 1.0)

    user.credits -= cost

    ai_message = Message(
        conversation_id=conversation_id,
        user_id=user_id,
        tenant_id=tenant_id,
        role="model",
        content=ai_response,
    )
    db.add(ai_message)
    db.commit()

    return {
        "conversation_id": conversation_id,
        "response": ai_response,
        "sources": sources,
        "is_cached": is_cached,
        "model_used": model_used,
        "credits_remaining": user.credits,
    }
