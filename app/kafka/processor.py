import asyncio
from ..logger import get_logger
from ..models.data import ScoreRec

logger = get_logger()

class MessageProcessor:
    def __init__(self, db_manager):
        self.db = db_manager
        self.processing = False
        self.lock = asyncio.Lock()

    async def process_messages(self, messages):
        """Process a batch of messages"""
        if not messages:
            return

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
                    logger.info(f"Successfully processed batch of {len(records)} scores")
                    return True
                except Exception as e:
                    logger.error(f"Error updating scores batch: {e}")
                    return False

    def set_processing(self, value):
        """Set the processing state"""
        self.processing = value 