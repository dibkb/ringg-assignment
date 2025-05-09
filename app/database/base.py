import asyncio
from typing import List, Optional
from ..models.data import Leader, RankInfo, ScoreRec
from ..kafka import KafkaProcessor
from .connection import DatabaseConnection
from .kafka_manager import KafkaManager
from .score_manager import ScoreManager
from ..config import kafka
from ..logger import get_logger

logger = get_logger()

class DatabaseManager:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.db_connection = DatabaseConnection()
            cls._instance.kafka_manager = KafkaManager()
            cls._instance.score_manager = None
            cls._instance.kafka_processor = None
            cls._instance._initialized = False
            cls._instance.max_retries = kafka.max_retries
            cls._instance.retry_delay = kafka.retry_delay
        return cls._instance

    @classmethod
    async def get_instance(cls):
        """Get the singleton instance of DatabaseManager"""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    async def initialize(self):
        """Initialize all components"""
        if self._initialized:
            return

        try:
            # Initialize database connection
            await self.db_connection.initialize()
            
            # Initialize Kafka manager
            await self.kafka_manager.initialize()
            
            # Initialize score manager with retry configuration
            self.score_manager = ScoreManager(
                self.db_connection,
                max_retries=self.max_retries,
                retry_delay=self.retry_delay
            )
            
            # Initialize Kafka processor
            self.kafka_processor = KafkaProcessor(self)
            await self.kafka_processor.start()
            
            self._initialized = True
            logger.info("Database manager initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database manager: {e}")
            await self.close()
            raise

    async def close(self):
        """Close all connections and stop processors"""
        if self.kafka_processor:
            await self.kafka_processor.stop()
        if self.db_connection:
            await self.db_connection.close()
        if self.kafka_manager:
            await self.kafka_manager.close()
        self._initialized = False
        # Reset the singleton instance
        DatabaseManager._instance = None

    async def process_score(self, rec: ScoreRec):
        """Process a score record asynchronously using Kafka"""
        if not self._initialized:
            await self.initialize()
            
        await self.kafka_manager.send_message(kafka.topic, rec.to_dict())

    async def update_scores_batch(self, records: List[ScoreRec]):
        """Update multiple scores in PostgreSQL"""
        if not self._initialized:
            await self.initialize()
        await self.score_manager.update_scores_batch(records)

    async def get_top_k(self, game_id: str, k: int) -> List[Leader]:
        """Get top k scores for a game"""
        if not self._initialized:
            await self.initialize()
        return await self.score_manager.get_top_k(game_id, k)

    async def get_rank(self, game_id: str, user_id: str) -> Optional[RankInfo]:
        """Get rank information for a user in a game"""
        if not self._initialized:
            await self.initialize()
        return await self.score_manager.get_rank(game_id, user_id)

    # Add these methods to maintain backward compatibility
    async def acquire_connection(self):
        """Acquire a database connection"""
        if not self._initialized:
            await self.initialize()
        return await self.db_connection.acquire_connection()

    async def acquire_connection_semaphore(self):
        """Acquire the connection semaphore"""
        if not self._initialized:
            await self.initialize()
        return await self.db_connection.acquire_connection_semaphore()

    def release_connection_semaphore(self):
        """Release the connection semaphore"""
        self.db_connection.release_connection_semaphore()

    @property
    def pool(self):
        """Get the database connection pool"""
        if not self._initialized:
            raise RuntimeError("Database manager not initialized")
        return self.db_connection.pool 