# Databricks notebook source
from pyspark.sql import functions as F
# Rate is a synthetic stream, NOT Kafka.
stream=spark.readStream.format('rate').option('rowsPerSecond',5).load()
orders=(stream.withColumn('event_id',F.concat(F.lit('EVT_STREAM_'),F.col('value')))
 .withColumn('order_id',F.concat(F.lit('ORD_'),(F.col('value')%100).cast('string')))
 .withColumn('order_status',F.when(F.col('value')%4==0,'created').when(F.col('value')%4==1,'confirmed').when(F.col('value')%4==2,'shipped').otherwise('delivered'))
 .withColumn('ingestion_timestamp',F.current_timestamp())
 .withColumn('source_system',F.lit('spark_rate_simulator'))
 .select('event_id','order_id','order_status',F.col('timestamp').alias('event_timestamp'),'ingestion_timestamp','source_system'))
# Create a Unity Catalog volume in your active catalog/schema, if permissions allow.
catalog=spark.sql('SELECT current_catalog()').first()[0]
schema=spark.sql('SELECT current_schema()').first()[0]
spark.sql('CREATE VOLUME IF NOT EXISTS streaming_checkpoints')
checkpoint=f'/Volumes/{catalog}/{schema}/streaming_checkpoints/order_stream'
q=(orders.writeStream.format('delta').outputMode('append').option('checkpointLocation',checkpoint).trigger(availableNow=True).toTable('bronze_streaming_orders'))
q.awaitTermination()
print('Streaming active:',q.isActive)
spark.table('bronze_streaming_orders').show(10,truncate=False)
# AvailableNow processes available input and stops. A rate source may produce
# few or no rows on very short runs; Kafka/file sources are better for replay demos.
