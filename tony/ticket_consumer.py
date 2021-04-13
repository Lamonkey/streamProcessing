import kafka 
consumer = kafka.KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
consumer.subscribe(['ticket_flights_stream'])
for msg in consumer:
    print(msg)