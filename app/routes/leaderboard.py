from fastapi import APIRouter, HTTPException, Query, Path
from ..models.response import LeaderboardResponse, RankResponse, LeaderboardEntry
from ..database import DatabaseManager
from ..logger import get_logger
from datetime import datetime, timedelta

logger = get_logger()
router = APIRouter()

@router.get("/games/{game_id}/leaders", response_model=LeaderboardResponse)
async def get_leaders(
    game_id: str = Path(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=100),
    window: int = Query(24, ge=1, le=24, description="Time window in hours to filter scores (1-24, default: 24)")
):
    """
    Get the top leaders for a specific game.
    
    - **game_id**: Unique identifier for the game
    - **limit**: Number of leaders to return (1-100)
    - **window**: Time window in hours to filter scores (1-24, default: 24)
    """
    try:
        logger.info(f"Getting leaders for game {game_id} with limit {limit} and window {window}h")
        
        # Get the singleton instance
        db = await DatabaseManager.get_instance()
        
        # Ensure database is initialized
        if not db.pool:
            await db.initialize()
            
        # Calculate timestamp for window
        now = datetime.utcnow()
        window_start = now - timedelta(hours=window)
        logger.info(f"Time window: {window_start} to {now}")
            
        leaders = await db.get_top_k(game_id, limit, window_start)
        entries = []
        
        for idx, leader in enumerate(leaders):
            rank_info = await db.get_rank(game_id, leader.user_id, window_start)
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

@router.get("/games/{game_id}/users/{user_id}/rank", response_model=RankResponse)
async def get_rank(
    game_id: str = Path(..., min_length=1, max_length=100),
    user_id: str = Path(..., min_length=1, max_length=100),
    window: int = Query(24, ge=1, le=24, description="Time window in hours to filter scores (1-24, default: 24)")
):
    """
    Get the rank information for a specific user in a game.
    
    - **game_id**: Unique identifier for the game
    - **user_id**: Unique identifier for the user
    - **window**: Time window in hours to filter scores (1-24, default: 24)
    """
    try:
        logger.info(f"Getting rank for user {user_id} in game {game_id} with window {window}h")
        
        # Get the singleton instance
        db = await DatabaseManager.get_instance()
        
        # Ensure database is initialized
        if not db.pool:
            await db.initialize()
            
        # Calculate timestamp for window
        now = datetime.utcnow()
        window_start = now - timedelta(hours=window)
        logger.info(f"Time window: {window_start} to {now}")
            
        rank_info = await db.get_rank(game_id, user_id, window_start)
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