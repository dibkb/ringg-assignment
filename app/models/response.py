from pydantic import BaseModel
from typing import List, Literal

class LeaderboardEntry(BaseModel):
    user_id: str
    score: int
    rank: int
    percentile: float

class LeaderboardResponse(BaseModel):
    game_id: str
    entries: List[LeaderboardEntry]

class RankResponse(BaseModel):
    game_id: str
    user_id: str
    rank: int
    score: int
    percentile: float

class HealthResponse(BaseModel):
    status: Literal["healthy"] = "healthy"
    uptime: float

class ScoreResponse(BaseModel):
    status: Literal["success"] = "success"
    message: str