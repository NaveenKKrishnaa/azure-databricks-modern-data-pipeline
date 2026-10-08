# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
# Demo of applying a CDC-like incremental microbatch. This is NOT log-based CDC.
updates=[('EVT007','ORD1001','CUST101','shipped',149.99,'2026-10-07 10:00:00','web'),('EVT008','ORD1002','CUST102','delivered',79.50,'2026-10-07 10:30:00','mobile'),('EVT009','ORD1005','CUST105','created',350.0,'2026-10-07 10:45:00','web')]
# Reuse schema from original table, excluding ingestion metadata.
schema=spark.table('bronze_order_events').select('event_id','order_id','customer_id','order_status','amount','event_timestamp','source_system').schema
batch=(spark.createDataFrame(updates,schema).withColumn('event_timestamp',F.to_timestamp('event_timestamp')).withColumn('ingestion_timestamp',F.current_timestamp()).withColumn('source_file',F.lit('synthetic_batch_002')))
# MERGE expects one latest source row per business key.
w=__import__('pyspark').sql.window.Window.partitionBy('order_id').orderBy(F.col('event_timestamp').desc(),F.col('event_id').desc())
latest=batch.withColumn('rn',F.row_number().over(w)).filter('rn=1').drop('rn')
(DeltaTable.forName(spark,'silver_current_orders').alias('t').merge(latest.alias('s'),'t.order_id=s.order_id').whenMatchedUpdateAll(condition='s.event_timestamp > t.event_timestamp').whenNotMatchedInsertAll().execute())
spark.table('silver_current_orders').select('order_id','order_status','event_timestamp').orderBy('order_id').show()
# Production design: also append validated/deduplicated updates to event history;
# this notebook only demonstrates current-state MERGE and is not a complete CDC pipeline.
