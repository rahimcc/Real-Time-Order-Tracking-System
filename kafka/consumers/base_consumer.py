from confluent_kafka import Consumer
import sys,os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS


from config import KAFKA_CONFIG
import json 


class BaseConsumer: 

    def __init__(self, group_id:str , topics: list):
        
        config = { 
            **KAFKA_CONFIG,
            'group.id': group_id,
            'auto.offset.reset': "earliest"
            }
        
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
    
    def process(self, message: dict):
        """
        Prosessing message
        """
        print(message)

    
    def run(self):

        print(f'Consumer started: ')

        
        try: 
            while True: 

                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    continue
                
                value = json.loads(msg.value().decode('utf-8'))
                self.process(value)
        except KeyboardInterrupt:
            print(f'\nStopping consumer')
        finally: 
            self.consumer.close()
        
    
