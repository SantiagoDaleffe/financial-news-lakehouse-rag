from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List, Dict, Any, Optional
from datetime import datetime

# --- INGESTION SCHEMAS ---
class NewsItem(BaseModel):
    text: str
    published_at: float 
    url: str
    tickers: List[str] = Field(default_factory=list)
    
    @field_validator('text')
    @classmethod
    def text_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('Document text cannot be empty or contain only whitespace.')
        return v.strip()

# --- ALERTS SCHEMAS ---
class AlertCreate(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10, description="Market symbol")
    target_price: float = Field(..., gt=0, description="Target price threshold in USD")
    condition: str = Field(..., pattern="^(above|below)$", description="Trigger condition")

class AlertResponse(BaseModel):
    id: int
    ticker: str
    target_price: float
    condition: str
    status: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
        
class MessageResponse(BaseModel):
    message: str
    ticker: Optional[str] = None

# --- CHAT & AGENT SCHEMAS ---
class ChatResponse(BaseModel):
    conversation_id: int
    response: str
    sources: List[Dict[str, Any]]
    is_cached: bool
    model_used: str
    credits_remaining: float
    
class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    conversation_id: Optional[int] = None
    model_override: Optional[str] = None
    
# AUTH SCHEMAS
class Token(BaseModel):
    access_token: str
    token_type: str