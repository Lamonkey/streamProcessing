from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('tickets', group_id='tickets')
for i in range(100):
    print("added topic")
    value = str(i)
    producer.send('tickets', value = bytes(value,'utf-8'))
    time.sleep(1)