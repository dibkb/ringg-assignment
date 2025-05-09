# leaderboard_service.py
import time
from .logger import get_logger
from .models.data import ScoreRec

from .models.response import HealthResponse, LeaderboardEntry, LeaderboardResponse, RankResponse, ScoreResponse
from .models.score import ScoreRequest

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import ORJSONResponse
from .database.main import DatabaseManager
# Configure logging
logger = get_logger()



# --- FastAPI App ---
app = FastAPI(
    default_response_class=ORJSONResponse,
    title="RingAI Assignment",
    description="High-performance leaderboard service with PostgreSQL and Kafka",
    version="1.0.0"
)

# Initialize Database Manager
db = DatabaseManager()

# Track application start time
start_time = time.time()

@app.on_event("startup")
async def startup_event():
    """Initialize database connections and start background tasks"""
    try:
        await db.initialize()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections"""
    try:
        await db.close()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

@app.post("/ingest", response_model=ScoreResponse, status_code=201)
async def ingest_score(data: ScoreRequest):
    """
    Ingest a new score into the leaderboard.
    
    - **user_id**: Unique identifier for the user
    - **game_id**: Unique identifier for the game
    - **score**: Non-negative score value
    - **timestamp**: Unix timestamp of when the score was achieved
    """
    try:
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

@app.get("/games/{game_id}/leaders", response_model=LeaderboardResponse)
async def get_leaders(
    game_id: str = Path(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Get the top leaders for a specific game.
    
    - **game_id**: Unique identifier for the game
    - **limit**: Number of leaders to return (1-100)
    """
    try:
        logger.info(f"Getting leaders for game {game_id} with limit {limit}")
        leaders = await db.get_top_k(game_id, limit)
        entries = []
        for idx, leader in enumerate(leaders):
            rank_info = await db.get_rank(game_id, leader.user_id)
            if rank_info is not None:
                entries.append(
                    LeaderboardEntry(
                        user_id=leader.user_id,
                        score=leader.score,
                        rank=idx + 1,
                        percentile=rank_info.percentile
                    )
                )
        response = LeaderboardResponse(game_id=game_id, entries=entries)
        logger.info(f"Successfully retrieved {len(entries)} leaders")
        return response
    except Exception as e:
        logger.error(f"Error getting leaders: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get leaderboard")

@app.get("/games/{game_id}/users/{user_id}/rank", response_model=RankResponse)
async def get_rank(
    game_id: str = Path(..., min_length=1, max_length=100),
    user_id: str = Path(..., min_length=1, max_length=100)
):
    """
    Get the rank information for a specific user in a game.
    
    - **game_id**: Unique identifier for the game
    - **user_id**: Unique identifier for the user
    """
    try:
        logger.info(f"Getting rank for user {user_id} in game {game_id}")
        rank_info = await db.get_rank(game_id, user_id)
        if rank_info is None:
            logger.warning(f"User {user_id} not found in game {game_id}")
            raise HTTPException(status_code=404, detail="User not found in leaderboard")
        
        response = RankResponse(
            game_id=game_id,
            user_id=user_id,
            rank=rank_info.rank,
            score=rank_info.score,
            percentile=rank_info.percentile
        )
        logger.info(f"Successfully retrieved rank: {response.dict()}")
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting rank: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get rank")

@app.get("/health", response_model=HealthResponse)
@app.head("/health")
async def health_check():
    """Health check endpoint"""
    try:
        response = HealthResponse(
            uptime=time.time() - start_time
        )
        logger.debug(f"Health check response: {response.dict()}")
        return response
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")

if __name__ == "__main__":
    import uvicorn
    
    workers = 4  # One worker per CPU core
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=workers,
        loop="uvloop",
        limit_concurrency=1000, 
        backlog=1024,
        http="httptools",
        ws="websockets",
        log_level="info",
        access_log=True
    )
