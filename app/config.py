from pydantic_settings import BaseSettings
import os
import json

class DatabaseConfig(BaseSettings):
    HOST: str = os.getenv('POSTGRES_HOST', 'localhost')
    PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    DATABASE: str = os.getenv('POSTGRES_DB', 'leaderboard')
    USER: str = os.getenv('POSTGRES_USER', 'postgres')
    PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'postgres')

database = DatabaseConfig()

class KafkaConfig(BaseSettings):
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic: str = 'scores'
    group_id: str = 'leaderboard_processor'
    producer_config: dict = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
        'request_timeout_ms': 1000,
        'retry_backoff_ms': 100,
        'security_protocol': "PLAINTEXT",
        'client_id': 'leaderboard-producer'
    }
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: int = 1

kafka = KafkaConfig()