import asyncpg
import asyncio
from ..config import database
from ..logger import get_logger

logger = get_logger()

class DatabaseConnection:
    def __init__(self):
        self.pool = None
        self._connection_semaphore = None
        self._request_semaphore = None
        self.MAX_CONCURRENT_REQUESTS = 5000
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize database connections"""
        if self._initialized:
            return

        async with self._init_lock:
            if self._initialized:  # Double check after acquiring lock
                return

            try:
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

                self._initialized = True
                logger.info("Database connection initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database connection: {e}")
                await self.close()
                raise

    async def _setup_connection(self, connection):
        """Setup connection with proper settings"""
        await connection.execute('SET statement_timeout = 30000')
        await connection.execute('SET idle_in_transaction_session_timeout = 30000')
        await connection.execute('SET lock_timeout = 10000')

    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
        self._initialized = False

    async def acquire_connection(self):
        """Acquire a database connection with semaphore"""
        if not self._initialized:
            await self.initialize()
        return await self.pool.acquire()

    async def acquire_request_semaphore(self):
        """Acquire the request semaphore"""
        return await self._request_semaphore.acquire()

    async def acquire_connection_semaphore(self):
        """Acquire the connection semaphore"""
        return await self._connection_semaphore.acquire()

    def release_request_semaphore(self):
        """Release the request semaphore"""
        self._request_semaphore.release()

    def release_connection_semaphore(self):
        """Release the connection semaphore"""
        self._connection_semaphore.release() 