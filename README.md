# Modern Data Engineering Pipeline — Databricks / PySpark / Delta Lake

**Portfolio learning project using synthetic order events. No employer data.**

## Implemented notebooks

1. `01_bronze_ingestion.py`: explicit schema, synthetic ingestion, audit columns, Delta table.
2. `02_silver_transformation.py`: type conversion, data-quality quarantine, event deduplication, latest order state.
3. `03_incremental_cdc_merge.py`: incremental upsert into current-state Delta table using `MERGE` (not log-based CDC).
4. `04_structured_streaming.py`: Spark Rate source → streaming transformations → Delta Bronze using `AvailableNow` and Unity Catalog volume checkpoint.
5. `05_gold_analytics.py`: order status metrics.

Run notebooks **in numerical order** in the same Databricks catalog/schema. These are Databricks source notebooks; import them into your workspace. They use demo overwrites for repeatable setup, not production append-only ingestion. Notebook 3 demonstrates MERGE against current state but does not append its events to historical Bronze/Silver; this is an explicitly documented limitation.

## Design-only extensions (not claimed as deployed)

Kafka/Debezium CDC → Spark Kafka connector → Delta Bronze → `foreachBatch` idempotent MERGE; Azure ADLS, ADF, Key Vault, Azure Functions; orchestration with Airflow; Terraform infrastructure automation. See [architecture](docs/architecture.md).

## Key engineering concepts

Delta Lake transactions, schema enforcement, auditability, data-quality quarantine, deterministic windows, business-key UPSERT, event-time vs processing-time, micro-batches, checkpoints, and source-vs-sink responsibilities.

## Example validations

```sql
SELECT COUNT(*) FROM bronze_order_events; -- 7
SELECT COUNT(*) FROM quarantine_order_events; -- 2
SELECT COUNT(*) FROM silver_order_events; -- 4
SELECT COUNT(*) FROM silver_current_orders; -- 3 after notebook 3
DESCRIBE HISTORY silver_current_orders;
```

## Limitations

- Streaming notebook uses Spark's synthetic Rate source, **not Kafka**.
- No Azure deployment, live CDC capture, orchestration, or production CI/CD is claimed.
- Demo synthetic status generator does not represent realistic order lifecycle sequencing.
- Local tests validate basic transformation logic only, not Databricks integration.
