from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time


def stream_pipeline(source):
    def udf(row):
        row = row.split(",")
        hour = int(row[1].split(" ")[0].split(":")[0])
        if row[1].split(" ")[1] == 'PM':
            if hour != 12:
                hour += 12
        if hour == 12 and row[1].split(" ")[1] == 'AM':
            hour = 0
        loc = row[2][2:]
        return (hour, loc), 1
    
    def updateFunc(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        # add the new values with the previous running count to get the new count
        return sum(newValues, runningCount)  
    target = (
        source.map(udf)
        .reduceByKey(lambda a, b: a + b)
        .updateStateByKey(updateFunc)
        .map(lambda x: (x[0][0], (x[1], x[0][1])))
        .reduceByKey(max)
        .transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True))
    )
    return target


master = "spark://localhost:7077"
appName = "dstream_uber"
brokers = "localhost:9092"
topic = "uber_stream"

sc = SparkContext(master, appName)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("./uber_checkpoints/")


from pyspark.streaming.kafka import KafkaUtils
directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#, "auto.offset.reset": "smallest"

# Input: (None, '7/1/2014,12:00:00 AM," 874 E 139th St Mott Haven, BX",,,')

source = directKafkaStream.map(lambda x: x[1])
target = stream_pipeline(source)

target.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
