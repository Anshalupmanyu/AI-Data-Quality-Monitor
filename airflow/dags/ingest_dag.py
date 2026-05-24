from datetime import timedelta
from datetime import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from confluent_kafka import Consumer

def consume_from_kafka():
    conf = {'bootstrap.servers': 'kafka:29092', 
    'group_id': 'airflow_ingest_group', 
    'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)
    consumer.subscribe(['orders'])

    postgres = PostgresHook(postgres_conn_id='postgres_default')
    for i in range(100):
        msg = consumer.poll(1.0)

        if msg is None:
            break
        elif msg.error():
            print(msg.error())
            continue
        
        order_data = json.loads(msg.value().decode('utf-8'))
        insert_query = """
            INSERT INTO raw_orders (order_id, customer_email, amount, product, status) 
            VALUES (%s, %s, %s, %s, %s)
        """

        postgres.run(insert_query, parameters=(
            order_data['order_id'],
            order_data['customer_email'],
            order_data['amount'],
            order_data['product'],
            order_data['status']
        ))
    consumer.close()


default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'kafka_to_postgres_ingest',
    default_args = default_args,
    schedule_interval='*/5 * * * *',
    catchup = False
) as dag:
    ingest_task = PythonOperator(
        task_id = 'ingest_kafka_messages',
        python_callable=consume_from_kafka
    )