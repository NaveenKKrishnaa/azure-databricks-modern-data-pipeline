# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
bronze=spark.table('bronze_order_events')
typed=bronze.withColumn('event_timestamp',F.to_timestamp('event_timestamp'))
validated=typed.withColumn('dq_error',
 F.when(F.col('event_id').isNull() | F.col('order_id').isNull(),F.lit('MISSING_KEY'))
 .when(F.col('customer_id').isNull(),F.lit('MISSING_CUSTOMER'))
 .when(F.col('amount').isNull() | (F.col('amount')<=0),F.lit('INVALID_AMOUNT'))
 .when(F.col('event_timestamp').isNull(),F.lit('INVALID_TIMESTAMP'))
 .otherwise(F.lit(None)))
quarantine=validated.filter(F.col('dq_error').isNotNull())
valid=validated.filter(F.col('dq_error').isNull()).drop('dq_error')
# Deterministic winner for repeated event_id, if source sends conflicts.
w=Window.partitionBy('event_id').orderBy(F.col('ingestion_timestamp').desc(),F.col('event_timestamp').desc())
silver=(valid.withColumn('rn',F.row_number().over(w)).filter('rn = 1').drop('rn'))
silver.write.format('delta').mode('overwrite').saveAsTable('silver_order_events')
quarantine.write.format('delta').mode('overwrite').saveAsTable('quarantine_order_events')
order_w=Window.partitionBy('order_id').orderBy(F.col('event_timestamp').desc(),F.col('event_id').desc())
current=(silver.withColumn('rn',F.row_number().over(order_w)).filter('rn = 1').drop('rn'))
current.write.format('delta').mode('overwrite').saveAsTable('silver_current_orders')
print('Silver events:',silver.count(),'Quarantined:',quarantine.count(),'Current orders:',current.count())
