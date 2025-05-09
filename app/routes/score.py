from fastapi import APIRouter, HTTPException
from ..models.score import ScoreRequest
from ..models.response import ScoreResponse
from ..models.data import ScoreRec
from ..database import DatabaseManager
from ..logger import get_logger

logger = get_logger()
router = APIRouter()

@router.post("/ingest", response_model=ScoreResponse, status_code=201)
async def ingest_score(data: ScoreRequest):
    """
    Ingest a new score into the leaderboard.
    
    - **user_id**: Unique identifier for the user
    - **game_id**: Unique identifier for the game
    - **score**: Non-negative score value
    - **timestamp**: Unix timestamp of when the score was achieved
    """
    try:
        # Get the singleton instance
        db = await DatabaseManager.get_instance()
        
        # Ensure database is initialized
        if not db.pool:
            await db.initialize()
            
        rec = ScoreRec(data.dict())
        await db.process_score(rec)
        return ScoreResponse(message="Score queued for processing")
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error ingesting score: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 