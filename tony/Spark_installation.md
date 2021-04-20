## Install Spark 2.4.7
Python version for pyspark: 3.7  
(Roughly follow: https://maelfabien.github.io/bigdata/SparkInstall/#)  
Spark: 2.4.7  
Java: 1.8  

1. Install Java
2. Install Scala
3. Download https://www.apache.org/dyn/closer.lua/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz  
Unzip: ```tar xvf spark-2.4.7-bin-hadoop2.7.tgz```
4. Relocate folder: ```mv spark-2.4.7-bin-hadoop2.7 /usr/local/apache-spark/2.4.7```
5. ```pip install pyspark==2.4.7```
6. source  
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export JRE_HOME=/Library/java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.7
export PATH=/usr/local/Cellar/apache-spark/2.4.7/bin:$PATH
export PYSPARK_PYTHON=/Users/hsinhanh19/anaconda3/bin/python
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
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/start-slave.sh spark://local:7077```   
  To Stop,  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-slave.sh```  
  ```/usr/local/Cellar/apache-spark/2.4.7/sbin/stop-master.sh``` 
  
8. ```spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 dstream.py```  
  (spark-streaming-kafka package: https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8)


## Problem Solved:
1. Set master ip: https://stackoverflow.com/questions/31166851/spark-standalone-cluster-slave-not-connecting-to-master  
2. Spark Standalone mode: https://spark.apache.org/docs/2.4.7/spark-standalone.html  
3. Spark Streaming Guide: https://spark.apache.org/docs/2.4.7/streaming-programming-guide.html  
