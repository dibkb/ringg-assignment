from typing import List, Optional
import asyncio
from ..models.data import Leader, RankInfo, ScoreRec
from ..logger import get_logger
from datetime import datetime

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

    async def get_top_k(self, game_id: str, k: int, window_timestamp: Optional[datetime] = None) -> List[Leader]:
        """Get top k scores for a game, optionally filtered by time window"""
        await self.db.acquire_connection_semaphore()
        try:
            async with self.db.pool.acquire() as conn:
                query = '''
                    WITH filtered_scores AS (
                        SELECT user_id, score, timestamp
                        FROM scores
                        WHERE game_id = $1
                '''
                params = [game_id]
                
                if window_timestamp is not None:
                    unix_timestamp = window_timestamp.timestamp()
                    query += ' AND timestamp >= $2'
                    params.append(unix_timestamp)
                
                query += '''
                    )
                    SELECT user_id, score, timestamp
                    FROM filtered_scores
                    ORDER BY score DESC
                    LIMIT $''' + str(len(params) + 1)
                params.append(k)
                
                rows = await conn.fetch(query, *params)
                
                if window_timestamp is not None:
                    filtered_rows = [row for row in rows if row['timestamp'] >= unix_timestamp]
                    rows = filtered_rows
                
                return [Leader(row['user_id'], row['score']) for row in rows]
        finally:
            self.db.release_connection_semaphore()

    async def get_rank(self, game_id: str, user_id: str, window_timestamp: Optional[datetime] = None) -> Optional[RankInfo]:
        """Get rank information for a user in a game, optionally filtered by time window"""
        await self.db.acquire_connection_semaphore()
        try:
            async with self.db.pool.acquire() as conn:
                # Base conditions for all queries
                base_conditions = 'game_id = $1'
                params = [game_id]
                
                if window_timestamp is not None:
                    unix_timestamp = window_timestamp.timestamp()
                    base_conditions += ' AND timestamp >= $2'
                    params.append(unix_timestamp)
                
                # Get user's score
                score_query = f'''
                    SELECT score, timestamp
                    FROM scores
                    WHERE {base_conditions} AND user_id = ${len(params) + 1}
                '''
                score_params = params + [user_id]
                score_row = await conn.fetchrow(score_query, *score_params)
                
                if not score_row:
                    return None

                if window_timestamp is not None:
                    if score_row['timestamp'] < unix_timestamp:
                        return None

                # Get total count and user's rank
                rank_query = f'''
                    SELECT COUNT(*) as total
                    FROM scores
                    WHERE {base_conditions} AND score > ${len(params) + 1}
                '''
                rank_params = params + [score_row['score']]
                rank_row = await conn.fetchrow(rank_query, *rank_params)

                rank = rank_row['total'] + 1
                
                # Get total count for percentile calculation
                total_query = f'''
                    SELECT COUNT(*)
                    FROM scores
                    WHERE {base_conditions}
                '''
                total = await conn.fetchval(total_query, *params)

                return RankInfo(
                    user_id=user_id,
                    score=score_row['score'],
                    rank=rank,
                    total=total
                )
        finally:
            self.db.release_connection_semaphore() 