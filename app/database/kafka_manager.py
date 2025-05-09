import asyncio
from aiokafka import AIOKafkaProducer
from fastapi import HTTPException
from ..config import kafka
from ..logger import get_logger

logger = get_logger()

class KafkaManager:
    def __init__(self):
        self.producer = None
        self.retry_count = 0
        self.max_retries = kafka.max_retries
        self.retry_delay = kafka.retry_delay
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize Kafka producer"""
        if self._initialized:
            return

        async with self._init_lock:
            if self._initialized:  # Double check after acquiring lock
                return

            while self.retry_count < self.max_retries:
                try:
                    self.producer = AIOKafkaProducer(
                        **kafka.producer_config
                    )
                    await self.producer.start()
                    self.retry_count = 0
                    self._initialized = True
                    logger.info("Kafka producer initialized successfully")
                    return
                except Exception as e:
                    self.retry_count += 1
                    logger.error(f"Failed to initialize Kafka producer (attempt {self.retry_count}/{self.max_retries}): {e}")
                    if self.retry_count < self.max_retries:
                        await asyncio.sleep(self.retry_delay * self.retry_count)
                    else:
                        logger.error("Max retries reached for Kafka producer initialization")
                        raise

    async def send_message(self, topic: str, value: dict):
        """Send a message to Kafka topic"""
        if not self._initialized:
            await self.initialize()

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                await self.producer.send(topic, value=value)
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

    async def close(self):
        """Close Kafka producer"""
        if self.producer:
            await self.producer.stop()
        self._initialized = False 