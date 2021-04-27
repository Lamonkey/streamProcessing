from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import time

kafka_topic_name = "ticket_flights_stream"
kafka_bootstrap_servers = 'localhost:9092'

def map_avg(x):
    split = x.split(",")
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

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))


    spark = SparkSession.\
        builder.\
        appName("ticket_avg").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4").\
        getOrCreate() 
#config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5").\
#     spark.conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4")

#     spark.sparkContext.setLogLevel("ERROR")

    KAFKA_TOPIC = 'ticket_flights_stream'
    KAFKA_BROKERS = '192.168.1.3:9092' #ip
    ZOOKEEPER = '192.168.1.3:2181' #ip
    
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 1) # 2 second window
    
    from pyspark.streaming.kafka import KafkaUtils
    kafkaStream = KafkaUtils.createStream(ssc, ZOOKEEPER, 'consumer_group_1', {KAFKA_TOPIC:1})
#     kafkaStream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {"metadata.broker.list": KAFKA_BROKERS})
    

#     ssc.checkpoint("./checkpoints")
    ssc.checkpoint("/tmp/spark_checkpoint/")
    initialStateRDD = sc.parallelize([(u'Economy', (0, 0)),
                                  (u'Comfort', (0, 0)),
                                  (u'Business', (0, 0))])
    out = kafkaStream.map(map_avg).mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .updateStateByKey(updateFunc, initialRDD=initialStateRDD) \
        .mapValues(lambda x: (x[0] / x[1], x[0], x[1]))
    
#     out.pprint()
    ssc.start()
#     time.sleep(15)
    ssc.awaitTermination()
#     ssc.stop(stopSparkContext=True, stopGraceFully=True) 

    
    
#     # Construct a streaming DataFrame that reads from test-topic
#     orders_df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("subscribe", kafka_topic_name) \
#         .option("startingOffsets", "latest") \
#         .load()

#     print("Printing Schema of orders_df: ")
#     orders_df.printSchema()

#     orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

#     # Define a schema for the orders data
#     # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
#     orders_schema = StructType() \
#         .add("order_id", StringType()) \
#         .add("order_product_name", StringType()) \
#         .add("order_card_type", StringType()) \
#         .add("order_amount", StringType()) \
#         .add("order_datetime", StringType()) \
#         .add("order_country_name", StringType()) \
#         .add("order_city_name", StringType()) \
#         .add("order_ecommerce_website_name", StringType())

#     # {'order_id': 1, 'order_product_name': 'Laptop', 'order_card_type': 'MasterCard',
#     # 'order_amount': 38.48, 'order_datetime': '2020-10-21 10:59:10', 'order_country_name': 'Italy',
#     # 'order_city_name': 'Rome', 'order_ecommerce_website_name': 'www.flipkart.com'}
#     orders_df2 = orders_df1\
#         .select(from_json(col("value"), orders_schema)\
#         .alias("orders"), "timestamp")

#     orders_df3 = orders_df2.select("orders.*", "timestamp")

#     # Simple aggregate - find total_order_amount by grouping country, city
#     orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
#         .agg({'order_amount': 'sum'}) \
#         .select("order_country_name", "order_city_name", col("sum(order_amount)") \
#         .alias("total_order_amount"))

#     print("Printing Schema of orders_df4: ")
#     orders_df4.printSchema()

#     # Write final result into console for debugging purpose
#     orders_agg_write_stream = orders_df4 \
#         .writeStream \
#         .trigger(processingTime='5 seconds') \
#         .outputMode("update") \
#         .option("truncate", "false")\
#         .format("console") \
#         .start()

#     orders_agg_write_stream.awaitTermination()

#     print("Stream Data Processing Application Completed.")