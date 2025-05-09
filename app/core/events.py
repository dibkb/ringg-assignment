from ..database import DatabaseManager
from ..logger import get_logger
import asyncio

logger = get_logger()
db = DatabaseManager()

async def startup_event():
    """Initialize database connections and start background tasks"""
    try:
        await db.initialize()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def shutdown_event():
    """Close database connections"""
    try:
        # Set a timeout for the shutdown process
        async with asyncio.timeout(5.0):
            await db.close()
            logger.info("Database connections closed")
    except asyncio.TimeoutError:
        logger.warning("Shutdown timed out, forcing closure")
        # Force close any remaining connections
        if hasattr(db, 'kafka_processor'):
            await db.kafka_processor.force_stop()
    except asyncio.CancelledError:
        logger.warning("Shutdown was cancelled, forcing closure")
        # Force close any remaining connections
        if hasattr(db, 'kafka_processor'):
            await db.kafka_processor.force_stop()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        # Ensure we still try to force close connections
        if hasattr(db, 'kafka_processor'):
            await db.kafka_processor.force_stop() 