from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# if __name__ == "__main__":
if len(sys.argv) != 3:
    print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
    sys.exit(-1)

sc = SparkContext(appName="PythonStreamingKafkaWordCount")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("./wc_checkpoints/")


zkQuorum, topic = sys.argv[1:]
# kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
brokers = "localhost:9092"
kvs = KafkaUtils.createDirectStream(ssc, ["wordcounttopic"], {"metadata.broker.list": brokers,"auto.offset.reset": "smallest"})
lines = kvs.map(lambda x: x[1])
# lines.pprint()
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()
ssc.awaitTermination()