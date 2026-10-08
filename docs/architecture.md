# Architecture and trade-offs

```text
Synthetic batch → Bronze Delta → validation + quarantine → Silver event history
                                                       └→ Silver current orders → Gold KPIs
Incremental event batch ─────────────────────────────────→ Delta MERGE (upsert)
Synthetic rate stream → Structured Streaming AvailableNow → Bronze streaming Delta
```

## Streaming CDC extension (not deployed)

Operational DB → log-based CDC (e.g., Debezium) → Kafka topic (key=order_id) → Spark Kafka source → parse payload + preserve topic/partition/offset → Bronze Delta → validation + dedup → foreachBatch MERGE → current-state Silver → Gold.

- Partition key `order_id` preserves per-order ordering within a Kafka partition, not global ordering.
- Checkpoints track Structured Streaming progress/state; store in persistent Unity Catalog volume.
- Watermarks bound state for supported event-time stateful operations; they are not a generic guarantee of accepting all late records.
- For `foreachBatch`, design idempotent writes because a failed batch can be retried.
- Source CDC must carry a trustworthy sequence/version and delete semantics; event timestamps alone are not always sufficient.
- Azure reference architecture: ADF orchestrates scheduled ingestion; ADLS Gen2 stores data; Azure Functions handles lightweight event-triggered logic; Key Vault manages secrets; Azure DevOps/GitHub Actions implement CI/CD.
- Databricks Free Edition is a learning environment, not proof of Azure deployment or Kafka connectivity.
