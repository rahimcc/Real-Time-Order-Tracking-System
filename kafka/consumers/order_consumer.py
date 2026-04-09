

from base_consumer import BaseConsumer

import sys 
import os 

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS
from config import TOPICS


class OrderConsumer(BaseConsumer):

    def __init__(self):
        super().__init__(group_id="order-tracking-group",topics=TOPICS['orders'])


    def process(self, message: dict):

        print(f'OrderID: {message['order_id']}')
        print(f'OrderStatus: {message['status']}')





if __name__ == "__main__":

    consumer = OrderConsumer()
    consumer.run()