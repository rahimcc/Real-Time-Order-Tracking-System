import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer
from config import KAFKA_CONFIG
import json


class BaseProducer: 

    def __init__(self):
        
        self.producer = Producer(KAFKA_CONFIG)

    
    def delivery_report(self, err , msg):
        if err:
            print(f'[ERROR] Delivery failed: {err}')

        else: 
            print(f'[OK] Message delivered to {msg.topic()} [partition {msg.partition()}]')

    
    def publish(self, topic, key, value):

        self.producer.produce(
            topic=topic,
            key=key,
            value = json.dumps(value),
            callback=self.delivery_report
        )

        self.producer.poll(0) 

    def flush(self):

        self.producer.flush()
