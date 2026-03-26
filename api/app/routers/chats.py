from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..models import ChatRequest, Conversation, Message
from ..security import get_current_user
from .alerts import get_db
from .agent import run_agent_with_history

router = APIRouter(tags=["chat"])


@router.post("/")
async def chat(
    request: ChatRequest,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Processes chat messages, maintains conversation history, and generates AI responses with context.
    """
    if not request.conversation_id:
        new_conversation = Conversation(
            user_id=user_id, title=request.message[:40] + "..."
        )
        db.add(new_conversation)
        db.flush()
        conversation_id = new_conversation.id
    else:
        conversation_id = request.conversation_id
        conversation = (
            db.query(Conversation)
            .filter(Conversation.id == conversation_id, Conversation.user_id == user_id)
            .first()
        )
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

    new_message = Message(
        conversation_id=conversation_id,
        user_id=user_id,
        role="user",
        content=request.message,
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

    ai_response, sources = await run_agent_with_history(
        request.message, message_history, user_id
    )

    ai_message = Message(
        conversation_id=conversation_id,
        user_id=user_id,
        role="model",
        content=ai_response,
    )
    db.add(ai_message)
    db.commit()

    return {
        "conversation_id": conversation_id,
        "response": ai_response,
        "sources": sources,
    }
