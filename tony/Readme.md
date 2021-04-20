## Kafka
1. Install Kafka(https://kafka.apache.org/quickstart)
2. In Kafka folder, start zookeeper and kafka server  
```bin/zookeeper-server-start.sh config/zookeeper.properties```  
```bin/kafka-server-start.sh config/server.properties```

3. Run ```python ticket_producer.py```, which includes sending records to a new topic
4. Run ```python ticket_consumer.py``` to retrieve data


Other useful commands:
* Create new topic

```bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_name```
* List all existed topics

```bin/kafka-topics.sh --list --zookeeper localhost:2181```
* Delete topic

```bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic remove-me```
