import kafka

producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    line = input("Enter data: ")
    if line == "pushall":
        with open('./data/uber_pickup.csv', 'r') as f:
            count = 0
            for data in f:
                producer.send('uber_stream', data.rstrip().encode())
                count += 1
            print(count, "records has been produced in 'uber_stream'")
        producer.flush()
        continue
    producer.send('uber_stream', line.rstrip().encode())
