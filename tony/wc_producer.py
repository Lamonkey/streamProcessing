import kafka

producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'])

with open('/Users/huiminhan/Documents/VT21Spring/CS5614BigDataEngineering/Project/Phase3/test_text.txt', 'r') as f:
    count = 0
    for line in f:
        producer.send('wordcounttopic', line.rstrip().encode())
        count += 1
    print(count, "records has been produced in 'wordcounttopic'")
producer.flush()


