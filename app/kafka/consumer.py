import asyncio
import json
from aiokafka import AIOKafkaConsumer
from ..logger import get_logger
from ..config import kafka

logger = get_logger()

class KafkaConsumer:
    def __init__(self):
        self.consumer = None
        self.retry_count = 0
        self.max_retries = kafka.max_retries
        self.retry_delay = kafka.retry_delay

    async def initialize(self):
        """Initialize Kafka consumer with retry logic"""
        while self.retry_count < kafka.max_retries:
            try:
                self.consumer = AIOKafkaConsumer(
                    kafka.topic,
                    bootstrap_servers=kafka.bootstrap_servers,
                    group_id=kafka.group_id,
                    enable_auto_commit=False,
                    auto_commit_interval_ms=1000,
                    max_poll_records=kafka.batch_size,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    max_poll_interval_ms=300000,
                    request_timeout_ms=30000,
                    retry_backoff_ms=1000,
                    security_protocol="PLAINTEXT",
                    client_id='leaderboard-consumer',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                await self.consumer.start()
                self.retry_count = 0
                logger.info("Kafka consumer initialized successfully")
                return True
            except Exception as e:
                self.retry_count += 1
                logger.error(f"Failed to initialize Kafka consumer (attempt {self.retry_count}/{self.max_retries}): {e}")
                if self.retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * self.retry_count)
                else:
                    logger.error("Max retries reached for Kafka consumer initialization")
                    raise

    async def get_messages(self, timeout_ms=100):
        """Get messages from Kafka"""
        if not self.consumer:
            return None
        return await self.consumer.getmany(timeout_ms=timeout_ms)

    async def commit(self):
        """Commit the current offset"""
        if self.consumer:
            await self.consumer.commit()

    async def stop(self):
        """Stop the consumer gracefully"""
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka consumer: {e}")

    async def force_stop(self):
        """Force stop the consumer"""
        if self.consumer:
            try:
                self.consumer._coordinator._coordination_task.cancel()
                await self.consumer.stop()
                logger.info("Kafka consumer force stopped")
            except Exception as e:
                logger.error(f"Error force stopping Kafka consumer: {e}") 