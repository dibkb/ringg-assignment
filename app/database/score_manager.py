from typing import List, Optional
import asyncio
from ..models.data import Leader, RankInfo, ScoreRec
from ..logger import get_logger

logger = get_logger()

class ScoreManager:
    def __init__(self, db_connection, max_retries: int = 3, retry_delay: int = 1):
        self.db = db_connection
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def update_scores_batch(self, records: List[ScoreRec]):
        """Update multiple scores in PostgreSQL in a single transaction"""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                await self.db.acquire_connection_semaphore()
                try:
                    async with self.db.pool.acquire() as conn:
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
                finally:
                    self.db.release_connection_semaphore()
            except Exception as e:
                retry_count += 1
                logger.error(f"Database error (attempt {retry_count}/{self.max_retries}): {e}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
                else:
                    raise

    async def get_top_k(self, game_id: str, k: int) -> List[Leader]:
        """Get top k scores for a game"""
        await self.db.acquire_connection_semaphore()
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT user_id, score
                    FROM scores
                    WHERE game_id = $1
                    ORDER BY score DESC
                    LIMIT $2
                ''', game_id, k)
                return [Leader(row['user_id'], row['score']) for row in rows]
        finally:
            self.db.release_connection_semaphore()

    async def get_rank(self, game_id: str, user_id: str) -> Optional[RankInfo]:
        """Get rank information for a user in a game"""
        await self.db.acquire_connection_semaphore()
        try:
            async with self.db.pool.acquire() as conn:
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

                return RankInfo(
                    user_id=user_id,
                    score=score_row['score'],
                    rank=rank,
                    total=total
                )
        finally:
            self.db.release_connection_semaphore() 