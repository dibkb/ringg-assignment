# leaderboard_service.py

import asyncio
import logging
import json
from typing import Dict, List, Optional, Literal
from collections import defaultdict
from sortedcontainers import SortedList
from datetime import datetime
import time
import os
import aiofiles
from pathlib import Path as PathLibPath
import asyncpg
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field, validator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'leaderboard'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'topic': 'scores',
    'group_id': 'leaderboard_processor'
}

BATCH_SIZE = 100
MAX_RETRIES = 3

# --- Pydantic Models ---
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

class ScoreResponse(BaseModel):
    status: Literal["success"] = "success"
    message: str

class LeaderboardEntry(BaseModel):
    user_id: str
    score: int
    rank: int

class LeaderboardResponse(BaseModel):
    game_id: str
    entries: List[LeaderboardEntry]

class RankResponse(BaseModel):
    game_id: str
    user_id: str
    rank: int
    score: int

class HealthResponse(BaseModel):
    status: Literal["healthy"] = "healthy"
    uptime: float

# --- Database Models ---
class ScoreRec:
    __slots__ = ('user_id', 'game_id', 'score', 'timestamp')
    def __init__(self, data: dict):
        self.user_id = data['user_id']
        self.game_id = data['game_id']
        self.score = int(data['score'])
        if data.get('timestamp') is None:
            self.timestamp = time.time()
        elif isinstance(data['timestamp'], datetime):
            self.timestamp = data['timestamp'].timestamp()
        elif isinstance(data['timestamp'], str):
            dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            self.timestamp = dt.timestamp()
        else:
            self.timestamp = float(data['timestamp'])

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'game_id': self.game_id,
            'score': self.score,
            'timestamp': self.timestamp
        }

class Leader:
    __slots__ = ('user_id', 'score')
    def __init__(self, user_id: str, score: int):
        self.user_id = user_id
        self.score = score

class RankInfo:
    __slots__ = ('user_id', 'score', 'rank', 'percentile')
    def __init__(self, user_id: str, score: int, rank: int, total: int):
        self.user_id = user_id
        self.score = score
        self.rank = rank
        self.percentile = 100 * (1 - (rank - 1) / total)

class KafkaProcessor:
    def __init__(self, db_manager):
        self.db = db_manager
        self.processing = False
        self.batch = []
        self.lock = asyncio.Lock()
        self.consumer = None
        self.producer = None

    async def start(self):
        """Start the Kafka processor"""
        self.processing = True
        self.consumer = AIOKafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            group_id=KAFKA_CONFIG['group_id'],
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=BATCH_SIZE
        )
        await self.consumer.start()
        asyncio.create_task(self._process_messages())

    async def stop(self):
        """Stop the Kafka processor"""
        self.processing = False
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    async def _process_messages(self):
        """Main message processing loop"""
        while self.processing:
            try:
                async with self.lock:
                    # Get messages in batches
                    messages = await self.consumer.getmany(timeout_ms=100)
                    for tp, msgs in messages.items():
                        if msgs:
                            records = []
                            for msg in msgs:
                                try:
                                    score_dict = json.loads(msg.value.decode())
                                    records.append(ScoreRec(score_dict))
                                except json.JSONDecodeError as e:
                                    logger.error(f"Invalid JSON in message: {e}")
                                except Exception as e:
                                    logger.error(f"Error processing message: {e}")

                            if records:
                                await self.db.update_scores_batch(records)
                                logger.info(f"Successfully processed batch of {len(records)} scores")

            except Exception as e:
                logger.error(f"Error in message processing: {e}")
                await asyncio.sleep(1)

# --- Database Manager ---
class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.kafka_processor = None
        self.producer = None

    async def initialize(self):
        """Initialize database connections and create tables"""
        # Initialize PostgreSQL connection pool with proper limits
        self.pool = await asyncpg.create_pool(
            **DB_CONFIG,
            min_size=5,
            max_size=20,
            command_timeout=30
        )

        # Initialize Kafka producer
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()

        # Create tables if they don't exist
        async with self.pool.acquire() as conn:
            # Set statement timeout at the session level
            await conn.execute('SET statement_timeout = 30000')
            
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
        try:
            # Send to Kafka topic
            await self.producer.send_and_wait(
                KAFKA_CONFIG['topic'],
                value=rec.to_dict()
            )
        except Exception as e:
            logger.error(f"Kafka error: {e}")
            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )

    async def update_scores_batch(self, records: List[ScoreRec]):
        """Update multiple scores in PostgreSQL in a single transaction"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for rec in records:
                    await conn.execute('''
                        INSERT INTO scores (user_id, game_id, score, timestamp)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id, game_id) 
                        DO UPDATE SET 
                            score = GREATEST(scores.score, $3),
                            timestamp = $4
                        WHERE scores.score < $3
                    ''', rec.user_id, rec.game_id, rec.score, rec.timestamp)

    async def get_top_k(self, game_id: str, k: int) -> List[Leader]:
        """Get top k scores for a game"""
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

# --- FastAPI App ---
app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Leaderboard Service",
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
    await db.initialize()
    logger.info("Database initialized")

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections"""
    await db.close()
    logger.info("Database connections closed")

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
        logger.info(f"Received ingest request: {data.dict()}")
        rec = ScoreRec(data.dict())
        await db.process_score(rec)
        response = ScoreResponse(message="Score queued for processing")
        logger.info(f"Successfully queued score: {response.dict()}")
        return response
    except HTTPException:
        raise
    except ValueError as e:
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
        leaders = await db.get_top_k(game_id, limit)
        entries = [
            LeaderboardEntry(
                user_id=leader.user_id,
                score=leader.score,
                rank=idx + 1
            )
            for idx, leader in enumerate(leaders)
        ]
        return LeaderboardResponse(game_id=game_id, entries=entries)
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
        rank_info = await db.get_rank(game_id, user_id)
        if rank_info is None:
            raise HTTPException(status_code=404, detail="User not found in leaderboard")
        
        return RankResponse(
            game_id=game_id,
            user_id=user_id,
            rank=rank_info.rank,
            score=rank_info.score
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting rank: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get rank")

@app.get("/health", response_model=HealthResponse)
@app.head("/health")
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        uptime=time.time() - start_time
    )

if __name__ == "__main__":
    import uvicorn
    
    # Optimize for 4-core system
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
        log_level="info"
    )
