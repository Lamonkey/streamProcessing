## Install Spark 2.4.7
1. Install Java
2. Install Scala
3. Download https://www.apache.org/dyn/closer.lua/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz  
Unzip: ```tar xvf spark-2.4.7-bin-hadoop2.7.tgz```
4. Relocate folder: ```mv spark-2.4.7-bin-hadoop2.7 /usr/local/apache-spark/2.4.7```
5. ```pip install pyspark==2.4.7```
6. source  
```export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export JRE_HOME=/Library/java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.7
export PATH=/usr/local/Cellar/apache-spark/2.4.7/bin:$PATH
export PYSPARK_PYTHON=/Users/hsinhanh19/anaconda3/bin/python```
