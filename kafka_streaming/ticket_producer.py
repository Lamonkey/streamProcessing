import kafka

producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'])

with open('ticket_flights.csv', 'r') as f:
    count = 0
    for line in f:
        producer.send('ticket_flights_stream', line.rstrip().encode())
        count += 1
    print(count, "records has been produced in 'ticket_flights_stream'")
producer.flush()