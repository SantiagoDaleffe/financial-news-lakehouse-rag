from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ..models import ChatRequest, Conversation, Message, User
from ..security import get_current_user
from .alerts import get_db
from .agent import run_agent_with_history

router = APIRouter(tags=["chat"])

MODEL_COSTS = {
    "gemini-3.1-pro-preview": 5.0,
    "gemini-2.5-pro": 3.0,
    "gemini-3-flash-preview": 1.0,
    "gemini-3.1-flash-lite": 0.5,
    "gemini-2.5-flash-lite": 0.5,
}

@router.post("/")
async def chat(
    request: ChatRequest,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Processes chat messages, maintains conversation history, generates AI responses,
    and manages user credits.
    """
    user = db.query(User).filter(User.id == user_id).first()
    
    # DEV
    if not user:
        user = User(id=user_id, email=f"{user_id}@test.com", credits=100.0)
        db.add(user)
        db.commit()
        db.refresh(user)
        
    if user.credits <= 0:
        raise HTTPException(status_code=402, detail='Insufficient tokens to perform the query.')
    
    
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

    ai_response, sources, is_cached, model_used = await run_agent_with_history(
        request.message, message_history, user_id, request.model_override
    )
    
    cost = 0.0
    if not is_cached and model_used != 'failed_all':
        cost = MODEL_COSTS.get(model_used, 1.0)
        
    user.credits -= cost

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
        "is_cached": is_cached,
        'model_used': model_used,
        'credits_remaining': user.credits
    }
