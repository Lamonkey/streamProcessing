# Spark Streaming + Kafka Integration
Kafka producer reads in .csv file and pushes data onto a topic. Spark streaming then works as a consumer to receive the streaming data and processes them.

- [Install Kafka](#kafka)
- [Install Spark 2.4.7](#spark)

## Environment
- Python version: 3.7 (pyspark only support python3.7)
- Spark: 2.4.7
- Java: 1.8

## Start Environment
### Step1: Start up Zookeeper and Kafka in two terminals
In `kafka_2.13-2.7.0` folder,

``bin/zookeeper-server-start.sh config/zookeeper.properties``

``bin/kafka-server-start.sh config/server.properties``

### Step2: Start up Spark Master and Workers
(Configuration: 2 workers on 1 node, 1 executor per worker)

```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-master.sh```
```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-slave.sh spark://localhost:7077```

Cluster overview

| Application     | URL                                      | Description                                                |
| --------------- | ---------------------------------------- | ---------------------------------------------------------- |
| Spark Driver    | [localhost:4040](http://localhost:4040/) | Spark Driver web ui                                        |
| Spark Master    | [localhost:8080](http://localhost:8080/) | Spark Master node                                          |
| Spark Worker I  | [localhost:8081](http://localhost:8081/) | Spark Worker node with 2 core and 2g of memory (default) |
| Spark Worker II | [localhost:8082](http://localhost:8082/) | Spark Worker node with 2 core and 2g of memory (default) |

### Step3: Start your program
(Use spark-streaming-kafka package, from https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8)

```spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 dstream.py```

### To shut down workers and servers
```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-slave.sh```  
```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-master.sh```

```Crtl+C``` on Kafka server terminal, then Zookeeper server.


# Kafka
1. Install Kafka(https://kafka.apache.org/quickstart)
2. In Kafka folder, start zookeeper and kafka server  
```bin/zookeeper-server-start.sh config/zookeeper.properties```  
```bin/kafka-server-start.sh config/server.properties```

3. ```pip install kafka-python```
4. Run ```python ticket_producer.py```, which includes sending records to a new topic
5. Run ```python ticket_consumer.py``` to retrieve data



### Other useful commands:
* Create new topic  
```bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_name```

* List all existed topics  
```bin/kafka-topics.sh --list --zookeeper localhost:2181```

* Delete topic  
```bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic remove-me```


# Spark
### Version
Python: 3.7  
Spark: 2.4.7  
Java: 1.8  
(Reference: https://maelfabien.github.io/bigdata/SparkInstall/#)

### Steps
1. Install Java
2. Install Scala
3. Download https://www.apache.org/dyn/closer.lua/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz  
   Unzip: ```tar xvf spark-2.4.7-bin-hadoop2.7.tgz```
4. Rename folder "spark-2.4.7-bin-hadoop2.7" to "2.4.7"  
   Relocate folder: ```mv 2.4.7 /usr/local/apache-spark/```
5. ```pip install pyspark==2.4.7```
6. Set PATH  
(Path depends on where your files stored)  
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export JRE_HOME=/Library/java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.7
export PATH=/usr/local/Cellar/apache-spark/2.4.7/bin:$PATH
export PYSPARK_PYTHON=/Users/yourMac/anaconda3/bin/python
```

7. Start Spark master and workers  
  2-workers-on-1-node Standalone Cluster (one executor per worker)  
  (https://mallikarjuna_g.gitbooks.io/spark/content/spark-standalone-example-2-workers-on-1-node-cluster.html)  
  ```cd /usr/local/Cellar/apache-spark/2.4.7/sbin/```  
  Set configuration first,  
  ```cd /usr/local/Cellar/apache-spark/2.4.7/conf/```  
  Create spark_env.sh  
  ```bash  
  SPARK_WORKER_CORES=2
  SPARK_WORKER_INSTANCES=2
  SPARK_WORKER_MEMORY=2g
  SPARK_MASTER_HOST=localhost
  SPARK_LOCAL_IP=localhost
  ```
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-master.sh```  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-slave.sh spark://localhost:7077```   
  To Stop,  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-slave.sh```  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-master.sh``` 
  
8. ```spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 dstream.py```  
  (spark-streaming-kafka package: https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8)


## Problem Solved:
1. Set master ip: https://stackoverflow.com/questions/31166851/spark-standalone-cluster-slave-not-connecting-to-master  
2. Spark Standalone mode: https://spark.apache.org/docs/2.4.7/spark-standalone.html  
3. Spark Streaming Guide: https://spark.apache.org/docs/2.4.7/streaming-programming-guide.html  
4. Spark Streaming + Kafka Integration: https://spark.apache.org/docs/2.4.7/streaming-kafka-0-8-integration.html
