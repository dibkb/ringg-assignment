# --- Pydantic Models ---
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

class ScoreRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=100)
    game_id: str = Field(..., min_length=1, max_length=100)
    score: int = Field(..., ge=0)
    timestamp: Optional[datetime] = None

    @validator('user_id', 'game_id')
    def validate_id(cls, v):
        if not v.strip():
            raise ValueError('ID cannot be empty or whitespace')
        return v.strip()

