import asyncpg
import asyncio
from typing import List, Optional
from fastapi import HTTPException
from aiokafka import AIOKafkaProducer

from ..config import database, kafka
from ..kafka.main import KafkaProcessor
from ..models.data import Leader, RankInfo, ScoreRec
from ..logger import get_logger

logger = get_logger()

# --- Database Manager ---
class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.kafka_processor = None
        self.producer = None
        self.retry_count = 0
        self.max_retries = kafka.max_retries
        self.retry_delay = kafka.retry_delay
        self._connection_semaphore = None
        self._request_semaphore = None
        self.MAX_CONCURRENT_REQUESTS = 5000

    async def initialize(self):
        """Initialize database connections and create tables"""
        # Initialize PostgreSQL connection pool with proper limits
        self.pool = await asyncpg.create_pool(
            host=database.HOST,
            port=database.PORT,
            database=database.DATABASE,
            user=database.USER,
            password=database.PASSWORD,
            min_size=20,
            max_size=100,
            command_timeout=10,
            max_inactive_connection_lifetime=300.0,
            setup=self._setup_connection
        )
        

        self._connection_semaphore = asyncio.Semaphore(50)
        self._request_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

        # Initialize Kafka producer
        while self.retry_count < self.max_retries:
            try:
                self.producer = AIOKafkaProducer(
                    **kafka.producer_config
                )
                await self.producer.start()
                self.retry_count = 0
                logger.info("Kafka producer initialized successfully")
                break
            except Exception as e:
                self.retry_count += 1
                logger.error(f"Failed to initialize Kafka producer (attempt {self.retry_count}/{self.max_retries}): {e}")
                if self.retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * self.retry_count)
                else:
                    logger.error("Max retries reached for Kafka producer initialization")
                    raise

        # Create tables if they don't exist
        async with self.pool.acquire() as conn:
            await conn.execute('SET statement_timeout = 10000')  # Reduced timeout
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS scores (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(100) NOT NULL,
                    game_id VARCHAR(100) NOT NULL,
                    score INTEGER NOT NULL,
                    timestamp DOUBLE PRECISION NOT NULL,
                    UNIQUE(user_id, game_id)
                )
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_scores_game_user 
                ON scores(game_id, user_id)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_scores_game_score 
                ON scores(game_id, score DESC)
            ''')

        # Initialize Kafka processor
        self.kafka_processor = KafkaProcessor(self)
        await self.kafka_processor.start()

    async def _setup_connection(self, connection):
        """Setup connection with proper settings"""
        await connection.execute('SET statement_timeout = 30000')
        await connection.execute('SET idle_in_transaction_session_timeout = 30000')
        await connection.execute('SET lock_timeout = 10000')

    async def close(self):
        """Close database connections"""
        if self.kafka_processor:
            await self.kafka_processor.stop()
        if self.pool:
            await self.pool.close()
        if self.producer:
            await self.producer.stop()

    async def process_score(self, rec: ScoreRec):
        """Process a score record asynchronously using Kafka"""
        async with self._request_semaphore:  # Limit concurrent requests
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    # Send to Kafka topic without waiting for acknowledgment
                    await self.producer.send(
                        kafka.topic,
                        value=rec.to_dict()
                    )
                    return
                except Exception as e:
                    retry_count += 1
                    logger.error(f"Kafka error (attempt {retry_count}/{self.max_retries}): {e}")
                    if retry_count < self.max_retries:
                        await asyncio.sleep(self.retry_delay * retry_count)
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail="Internal server error"
                        )

    async def update_scores_batch(self, records: List[ScoreRec]):
        """Update multiple scores in PostgreSQL in a single transaction"""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                async with self._connection_semaphore:  # Limit concurrent DB operations
                    async with self.pool.acquire() as conn:
                        async with conn.transaction():
                            # Process in smaller chunks to prevent memory issues
                            chunk_size = 50
                            for i in range(0, len(records), chunk_size):
                                chunk = records[i:i + chunk_size]
                                # Use executemany for better performance
                                await conn.executemany('''
                                    INSERT INTO scores (user_id, game_id, score, timestamp)
                                    VALUES ($1, $2, $3, $4)
                                    ON CONFLICT (user_id, game_id) 
                                    DO UPDATE SET 
                                        score = GREATEST(scores.score, $3),
                                        timestamp = $4
                                    WHERE scores.score < $3
                                ''', [(rec.user_id, rec.game_id, rec.score, rec.timestamp) for rec in chunk])
                return
            except Exception as e:
                retry_count += 1
                logger.error(f"Database error (attempt {retry_count}/{self.max_retries}): {e}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
                else:
                    raise

    async def get_top_k(self, game_id: str, k: int) -> List[Leader]:
        """Get top k scores for a game"""
        async with self._connection_semaphore:  # Limit concurrent DB operations
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT user_id, score
                    FROM scores
                    WHERE game_id = $1
                    ORDER BY score DESC
                    LIMIT $2
                ''', game_id, k)
                return [Leader(row['user_id'], row['score']) for row in rows]

    async def get_rank(self, game_id: str, user_id: str) -> Optional[RankInfo]:
        """Get rank information for a user in a game"""
        async with self._connection_semaphore:  # Limit concurrent DB operations
            async with self.pool.acquire() as conn:
                # Get user's score
                score_row = await conn.fetchrow('''
                    SELECT score
                    FROM scores
                    WHERE game_id = $1 AND user_id = $2
                ''', game_id, user_id)
                
                if not score_row:
                    return None

                # Get total count and user's rank
                rank_row = await conn.fetchrow('''
                    SELECT COUNT(*) as total
                    FROM scores
                    WHERE game_id = $1 AND score > $2
                ''', game_id, score_row['score'])

                rank = rank_row['total'] + 1
                total = await conn.fetchval('''
                    SELECT COUNT(*)
                    FROM scores
                    WHERE game_id = $1
                ''', game_id)

                return RankInfo(user_id, score_row['score'], rank, total)
