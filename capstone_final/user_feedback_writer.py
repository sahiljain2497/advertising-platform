from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create spark session
spark = SparkSession  \
        .builder  \
        .appName("KafkaToHiveUserFeedbackArchive")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

#readStream from kafka server
feedbackRaw = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","localhost:9092")  \
        .option("subscribe","user-feedback")  \
        .load()

#define schema for the stream data
jsonSchema = StructType()  \
        .add('campaign_id', StringType())  \
        .add('user_id', StringType())  \
        .add('request_id', StringType())  \
        .add('click', IntegerType())  \
        .add('view', IntegerType()) \
        .add('acquisition', IntegerType()) \
        .add('auction_cpm', DoubleType())  \
        .add('auction_cpc', DoubleType()) \
        .add('auction_cpa', DoubleType()) \
        .add('target_age_range', StringType())  \
        .add('target_location',StringType()) \
        .add('target_gender',StringType()) \
        .add('target_income_bucket',StringType()) \
        .add('target_device_type', StringType()) \
        .add('campaign_start_time', StringType()) \
        .add('campaign_end_time', StringType()) \
        .add('user_action', StringType()) \
        .add('expenditure', DoubleType()) \
        .add('timestamp', TimestampType())

#casting the json data
feedbackStream = feedbackRaw.select(from_json(col("value").cast('string'),jsonSchema).alias('data')).select("data.*")

#for testing output with help of console
# feedbackStream.writeStream \
#   .outputMode('append')  \
#   .format('console')  \
#   .option('truncate', "false")  \
#   .trigger(processingTime='1 minute') \
#   .start()

#write stream as csv in hdfs
feedbackStream.writeStream \
  .format("csv") \
  .outputMode("append") \
  .option("truncate", "false") \
  .option("path", "/tmp/feedback") \
  .option("checkpointLocation", "/tmp/feedback") \
  .trigger(processingTime="1 minute") \
  .start() \
  .awaitTermination()