import os

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aviation_data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)