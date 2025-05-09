import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_logger():
    return logger
