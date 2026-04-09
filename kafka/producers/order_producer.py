import uuid
import time
import random
from datetime import datetime
from faker import Faker
from base_producer import BaseProducer
import sys,os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS




fake = Faker()


PRODUCTS = [ 
    { 'name': "Wireless Headphones", 'price': 49.99 }, 
    { 'name': "Mechanical Keyboard", 'price': 89.99 },
    { 'name': "USB-C Hub", 'price': 29.99 },
    { 'name': "Webcam HD", 'price': 69.99 },
    { 'name': "Monitor Stand", 'price': 39.99}
]

STATUSES = [ 
    'order_placed',
    'order_packed',
    'order_packed', 
    'order_shipped',
    'out_for_delivery',
    'delivered'
]


class OrderProducer(BaseProducer):

    def create_order_event(self, status="order_placed"):
        
        product = random.choice(PRODUCTS)

        return { 
            'order_id': f'ORD-{uuid.uuid4().hex[:6].upper()}',
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'customer': fake.name(),
            'email': fake.email(),
            'product': product['name'],
            'quanitity': random.randint(1,5),
            'price': product['price'],
            'city': fake.city()
        }
    
    def simulate_order_lifecycle(self):
        """Publish  all status stages for a single order."""

        order = self.create_order_event()
        order_id = order['order_id']

        for status in STATUSES:

            order['status'] = status
            order['timestamp'] = datetime.now().isoformat()
            self.publish(topic=TOPICS['orders'], key=order_id, value=order)
            print(f'{order_id} -> {status}')
            time.sleep(1)

    def run(self, num_orders, interval=2): 

        for i in range(num_orders): 
            
            print(f'Starting order producer - sending {num_orders} orders...\n')

            self.simulate_order_lifecycle()
            time.sleep(interval)

        self.flush()

        print("All orders are sent.")

if __name__ == "__main__": 
    
    producer = OrderProducer()
    producer.run(num_orders=3)


