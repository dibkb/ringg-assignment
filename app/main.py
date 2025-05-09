# leaderboard_service.py
import time

from .routes import leaderboard,score,health
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

# Track application start time
start_time = time.time()

@app.on_event("startup")
async def startup_event():
    """Initialize database connections and start background tasks"""
    try:
        db = await DatabaseManager.get_instance()
        await db.initialize()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections"""
    try:
        db = await DatabaseManager.get_instance()
        await db.close()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

app.include_router(health.router, tags=["health"])
app.include_router(leaderboard.router, tags=["leaderboard"])
app.include_router(score.router, tags=["score"])

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
