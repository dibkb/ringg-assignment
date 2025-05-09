import asyncio
from ..logger import get_logger
from .consumer import KafkaConsumer
from .processor import MessageProcessor

logger = get_logger()

class KafkaProcessor:
    def __init__(self, db_manager):
        self.consumer = KafkaConsumer()
        self.processor = MessageProcessor(db_manager)
        self.processing = False

    async def start(self):
        """Start the Kafka processor"""
        self.processing = True
        await self.consumer.initialize()
        asyncio.create_task(self._process_messages())

    async def _process_messages(self):
        """Main message processing loop with error handling and reconnection logic"""
        while self.processing:
            try:
                async with self.processor.lock:
                    if not self.consumer.consumer:
                        logger.info("Consumer not initialized, attempting to initialize...")
                        await self.consumer.initialize()
                        continue

                    messages = await self.consumer.get_messages()
                    
                    if not messages:
                        continue

                    if await self.processor.process_messages(messages):
                        await self.consumer.commit()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in message processing: {e}")
                if isinstance(e, (ConnectionError, asyncio.TimeoutError)):
                    if self.consumer.consumer:
                        try:
                            await self.consumer.stop()
                        except Exception as stop_error:
                            logger.error(f"Error stopping consumer during reconnection: {stop_error}")
                    self.consumer.consumer = None
                    await asyncio.sleep(self.consumer.retry_delay)
                else:
                    await asyncio.sleep(1)

    async def stop(self):
        """Stop the Kafka processor gracefully"""
        logger.info("Stopping Kafka processor...")
        self.processing = False
        await self.consumer.stop()
        logger.info("Kafka processor stopped")

    async def force_stop(self):
        """Force stop the Kafka processor without waiting for graceful shutdown"""
        logger.warning("Force stopping Kafka processor...")
        self.processing = False
        
        # Cancel any pending tasks
        for task in asyncio.all_tasks():
            if task != asyncio.current_task():
                task.cancel()
        
        await self.consumer.force_stop()
        logger.info("Kafka processor force stopped") 