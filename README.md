# Online Convertor
For this project, we designed an online convertor for translating batch processing code to stream processing code. The tool is available at:
https://streamprocessing.herokuapp.com/simulateStream/

source code for frontend is at frontend branch.

Please refer to "Auto-refactor" folder for the source code for auto-refactor

kafka is used to test generated stream processing code. To set up Kafka, see the following steps.


# Spark Streaming + Kafka Integration
Kafka producer reads in .csv file and pushes data onto a topic. Spark streaming then works as a consumer to receive the streaming data and processes them.

- [Install Kafka](#kafka)
- [Install Spark 2.4.7](#spark)
- [Problem Solved](#problem-solved)

## Environment
- Python version: 3.7 (pyspark only support python3.7)
- Spark: 2.4.7
- Java: 1.8

## Run your program

### Preparation

Please check the ``spark-up.sh`` and ``spark-stop.sh`` scripts to make sure the path are correct for your project

Only run the following command when you are the first time to run the ``spark-up.sh`` and ``spark-stop.sh`` shell script.

``chmod +x ./spark-up.sh``
``chmod +x ./spark-stop.sh``

### Step1: Start up Zookeeper and Kafka, start up spark master and workers

run ``spark-up.sh``

### Step2: Push data onto a topic through producer
Run ```python ticket_producer.py```, which includes sending records to a new topic


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

run ``spark-stop.sh``

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
- Python: 3.7  
- Spark: 2.4.7  
- Java: 1.8  

(Reference: https://maelfabien.github.io/bigdata/SparkInstall/#)

### Step1: Installation
- Install Java
- Install Scala
- Install Spark 2.4.7  
   1. Download https://www.apache.org/dyn/closer.lua/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz  
   2. Unzip: ```tar xvf spark-2.4.7-bin-hadoop2.7.tgz```  
   3. Rename folder "spark-2.4.7-bin-hadoop2.7" to "2.4.7"  
   4. Relocate folder: ```mv 2.4.7 /usr/local/apache-spark/```
- Install pyspark  
   ```pip install pyspark==2.4.7```
- Set PATH  
   (Path depends on where your files stored)  
   ```bash
   export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
   export JRE_HOME=/Library/java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/
   export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.7
   export PATH=/usr/local/Cellar/apache-spark/2.4.7/bin:$PATH
   export PYSPARK_PYTHON=/Users/yourMac/anaconda3/bin/python
   ```

### Step2: Set Configuration for Spark master and workers
(Reference from: https://mallikarjuna_g.gitbooks.io/spark/content/spark-standalone-example-2-workers-on-1-node-cluster.html)

Configuration:  
- 2 workers on 1 node  
- Each worker with 2 cores
- Each worker with 2g memory
  
Go to config and create new configuration file,  
```cd /usr/local/Cellar/apache-spark/2.4.7/conf/```  
  
Create `spark-env.sh` and save it, 
```bash  
SPARK_WORKER_CORES=2
SPARK_WORKER_INSTANCES=2
SPARK_WORKER_MEMORY=2g
SPARK_MASTER_HOST=localhost
SPARK_LOCAL_IP=localhost
```

### Step3: Start Spark master and workers  
  ```cd /usr/local/Cellar/apache-spark/2.4.7/sbin/```  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-master.sh```  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-slave.sh spark://localhost:7077```   
   
### Step4: Start your program
(Apply spark-streaming-kafka package: https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8)

```spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 dstream.py```  


# Problem Solved:
1. Set master ip: https://stackoverflow.com/questions/31166851/spark-standalone-cluster-slave-not-connecting-to-master  
2. Spark Standalone mode: https://spark.apache.org/docs/2.4.7/spark-standalone.html  
3. Spark Streaming Guide: https://spark.apache.org/docs/2.4.7/streaming-programming-guide.html  
4. Spark Streaming + Kafka Integration: https://spark.apache.org/docs/2.4.7/streaming-kafka-0-8-integration.html
