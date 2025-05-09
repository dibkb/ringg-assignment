import asyncio
import json
from aiokafka import AIOKafkaConsumer
from ..logger import get_logger
from ..config import kafka
from ..models.data import ScoreRec

logger = get_logger()

class KafkaProcessor:
    def __init__(self, db_manager):
        self.db = db_manager
        self.processing = False
        self.batch = []
        self.lock = asyncio.Lock()
        self.consumer = None
        self.producer = None
        self.retry_count = 0
        self.max_retries = kafka.max_retries
        self.retry_delay = kafka.retry_delay

    async def start(self):
        """Start the Kafka processor"""
        self.processing = True
        await self._initialize_consumer()
        asyncio.create_task(self._process_messages())

    async def _initialize_consumer(self):
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
                return
            except Exception as e:
                self.retry_count += 1
                logger.error(f"Failed to initialize Kafka consumer (attempt {self.retry_count}/{self.max_retries}): {e}")
                if self.retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * self.retry_count)
                else:
                    logger.error("Max retries reached for Kafka consumer initialization")
                    raise

    async def _process_messages(self):
        """Main message processing loop with error handling and reconnection logic"""
        while self.processing:
            try:
                async with self.lock:
                    if not self.consumer:
                        logger.info("Consumer not initialized, attempting to initialize...")
                        await self._initialize_consumer()
                        continue

                    messages = await self.consumer.getmany(timeout_ms=100)
                    
                    if not messages:
                        continue
                        
                    for tp, msgs in messages.items():
                        if not msgs:
                            continue
                            
                        records = []
                        for msg in msgs:
                            try:
                                score_dict = msg.value
                                records.append(ScoreRec(score_dict))
                            except Exception as e:
                                logger.error(f"Error processing message: {e}")

                        if records:
                            try:
                                await self.db.update_scores_batch(records)
                                await self.consumer.commit()
                                logger.info(f"Successfully processed batch of {len(records)} scores")
                            except Exception as e:
                                logger.error(f"Error updating scores batch: {e}")
                                continue

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in message processing: {e}")
                if isinstance(e, (ConnectionError, asyncio.TimeoutError)):
                    if self.consumer:
                        try:
                            await self.consumer.stop()
                        except Exception as stop_error:
                            logger.error(f"Error stopping consumer during reconnection: {stop_error}")
                    self.consumer = None
                    await asyncio.sleep(self.retry_delay)
                else:
                    await asyncio.sleep(1)

    async def stop(self):
        """Stop the Kafka processor"""
        logger.info("Stopping Kafka processor...")
        self.processing = False
        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka consumer: {e}")
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")
        logger.info("Kafka processor stopped")