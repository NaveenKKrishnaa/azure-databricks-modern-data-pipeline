# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
rows = [
 ('EVT001','ORD1001','CUST101','created',149.99,'2026-10-07 09:01:00','web'),
 ('EVT002','ORD1002','CUST102','created',79.50,'2026-10-07 09:02:00','mobile'),
 ('EVT003','ORD1001','CUST101','confirmed',149.99,'2026-10-07 09:04:00','web'),
 ('EVT004','ORD1003',None,'created',220.0,'2026-10-07 09:05:00','mobile'),
 ('EVT005','ORD1002','CUST102','shipped',79.50,'2026-10-07 09:15:00','mobile'),
 ('EVT005','ORD1002','CUST102','shipped',79.50,'2026-10-07 09:15:00','mobile'),
 ('EVT006','ORD1004','CUST104','created',-25.0,'2026-10-07 09:20:00','web'),
]
schema=StructType([StructField('event_id',StringType(),False),StructField('order_id',StringType(),False),StructField('customer_id',StringType(),True),StructField('order_status',StringType(),True),StructField('amount',DoubleType(),True),StructField('event_timestamp',StringType(),True),StructField('source_system',StringType(),True)])
bronze=(spark.createDataFrame(rows,schema).withColumn('ingestion_timestamp',F.current_timestamp()).withColumn('source_file',F.lit('synthetic_batch_001')))
# Overwrite is intentional for repeatable DEMO setup; production bronze should retain history.
bronze.write.format('delta').mode('overwrite').saveAsTable('bronze_order_events')
print('Bronze rows:',spark.table('bronze_order_events').count())
