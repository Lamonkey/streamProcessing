
from kafka import KafkaProducer
import time
import csv
producer = KafkaProducer(bootstrap_servers='localhost:9092')
with open('data/ticket_flights.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
        ticket = (', '.join(row))
        producer.send('tickets', value = bytes(ticket,'utf-8'))
        time.sleep(1)


# for i in range(100):
#     print("added topic")
#     value = str(i)
#     producer.send('tickets', value = bytes(value,'utf-8'))
#     time.sleep(1)