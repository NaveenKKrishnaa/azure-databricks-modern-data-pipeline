# Databricks notebook source
from pyspark.sql import functions as F
current=spark.table('silver_current_orders')
gold=(current.groupBy('order_status').agg(F.count('*').alias('order_count'),F.round(F.sum('amount'),2).alias('total_order_amount')))
gold.write.format('delta').mode('overwrite').saveAsTable('gold_order_status_summary')
spark.table('gold_order_status_summary').show()
