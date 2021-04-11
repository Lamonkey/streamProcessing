from kafka import KafkaConsumer
consumer = KafkaConsumer('tickets', group_id='tickets')
for mes in consumer:
        print(mes)