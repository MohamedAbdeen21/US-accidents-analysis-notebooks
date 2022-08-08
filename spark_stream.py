import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit

spark = SparkSession \
        .builder \
        .master("local[6]") \
        .appName('Stream w/ Pyspark') \
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet('./Data/parquet/stream_df.parquet')
df.createOrReplaceTempView('stream')

try:
    start_time = time.time()
    last_run = 0
    while last_run <= 3600:
        now = time.time() - start_time
        result = spark.sql(f'SELECT * FROM stream WHERE Stream_Time BETWEEN {last_run} AND {now}')
        last_run = now

        # sqlite3 returns sqlite3.Row objects, need to call dict() on each object
        if not result.isEmpty():
               result.selectExpr("CAST(ID AS STRING)", "to_json(struct(*)) AS value") \
                    .filter("Event == 'Start'") \
                    .write \
                    .format('kafka') \
                    .option('kafka.bootstrap.servers','localhost:9092') \
                    .option('topic','quickstart-events') \
                    .save()

        print(last_run)
except KeyboardInterrupt:
    pass
