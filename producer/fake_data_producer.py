import os
import random
import uuid
import json

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


