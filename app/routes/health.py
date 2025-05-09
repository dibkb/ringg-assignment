import time
from fastapi import APIRouter, HTTPException
from ..models.response import HealthResponse
from ..logger import get_logger

logger = get_logger()
router = APIRouter()

# Track application start time
start_time = time.time()

@router.get("/health", response_model=HealthResponse)
@router.head("/health")
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