#!/bin/bash

osascript -e 'tell app "Terminal"
    do script "kafka_2.13-2.7.0/bin/zookeeper-server-start.sh kafka_2.13-2.7.0/config/zookeeper.properties"
end tell'

osascript -e 'tell app "Terminal"
    do script "kafka_2.13-2.7.0/bin/kafka-server-start.sh kafka_2.13-2.7.0/config/server.properties"
end tell'

# osascript -e 'tell app "Terminal"
#     do script "python /Users/huiminhan/Documents/VT21Spring/CS5614BigDataEngineering/Project/Phase3/ticket_producer.py"
# end tell'

/usr/local/Cellar/apache-spark/2.4.7/sbin/start-master.sh

/usr/local/Cellar/apache-spark/2.4.7/sbin/start-slave.sh spark://localhost:7077

wc_producer = python $SPARK_HOME/wc_producer.py 

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 kafka_wordcount.py zookeeper_server:2181 wordcounttopic

