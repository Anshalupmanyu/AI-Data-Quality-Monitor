import os
import random
import uuid
import json
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers':os.getenv('KAFKA_BROKER_URL','localhost:9092')}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Success! Message delivered to topic: {msg.topic()}")

def fake_order():
    return {
        "order_id":str(uuid.uuid4()),
        "customer_email":f"user{random.randint(1,1000)}@gmail.com",
        "amount":round(random.uniform(10.0,300.0),2),
        "product":random.choice(["Laptop","Mouse","Keyboard","Monitor","Headphones","webcam"]),
        "status":random.choice(["PENDING","SHIPPED","DELIVERED"])
    }
batch_count = 0
while True:
    order = fake_order()
    if batch_count % 7 == 0:
        order['amount'] = 100000

    if batch_count % 9 == 0:
        order['customer_email'] = None
    
    if batch_count % 13 == 0:
        order['product'] = None
    
    if batch_count % 10 == 0:
        order['status'] = None
    
    if batch_count % 11 == 0:
        order['order_id'] = 'DUPLICATE-ID-00000'
    
    batch_count += 1
    print(f"Sending order: {order}")
    order_bytes = json.dumps(order).encode('utf-8')
    producer.produce('orders', value=order_bytes, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)


