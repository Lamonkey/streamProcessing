from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time


def map_avg(consumer_record):
    # consumer_record example: (tuple)
    # (None, '0005432159776,30625,Business,42100')
    value = consumer_record[1]
    split = value.split(",")
    return split[2], float(split[3])

def updateFunc(new_values, running_tuple):
    '''
    new_values: values in current data
    running_tuple: values in states
    '''
    new_sum = [field[0] for field in new_values]
    new_count = [field[1] for field in new_values]
    running_sum, running_count = running_tuple

    return sum(new_sum, running_sum), sum(new_count, running_count)


master = "spark://localhost:7077"
appName = "grep_stream"
brokers = "localhost:9092"

sc = SparkContext(master, appName)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("./checkpoints/")

error_log = sc.textFile("/data/access.log.txt")
error_lines = error_log.filter(lambda row: "android" in row.lower()).take(10)
error_count = error_log.count()

print("Total: " + str(error_count) + " lines.")
for line in error_lines:
    print(line)


from pyspark.streaming.kafka import KafkaUtils
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["ticket_flights_stream"], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"})
#, "auto.offset.reset": "smallest"

initialStateRDD = sc.parallelize([(u'Economy', (0, 1)),
                                  (u'Comfort', (0, 1)),
                                  (u'Business', (0, 1))])
# out = directKafkaStream
# (None, '0005432159776,30625,Business,42100')
# (None, '0005435212351,30625,Business,42100')
# (None, '0005435212386,30625,Business,42100')
# (None, '0005435212381,30625,Business,42100')
# ...

out = directKafkaStream.map(map_avg).mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .updateStateByKey(updateFunc, initialRDD=initialStateRDD) \
        .mapValues(lambda x: (x[0] / x[1], x[0], x[1]))

out.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

# ssc.start()
# time.sleep(60)
# ssc.stop(stopSparkContext=True, stopGraceFully=True) 