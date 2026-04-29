# Fivetran Operations Lakeview Dashboard

Databricks Lakeview (AI/BI) dashboard showing sync health, performance, and MAR analytics for a Fivetran destination. Built on top of the [Fivetran Platform Connector](https://fivetran.com/docs/logs/fivetran-platform) quickstart dbt transformations.

## Dashboard tabs

| Tab | Description |
|---|---|
| **Operations Overview** | Connector health status, errors since last sync, status detail table |
| **Connector Health** | Record modifications, API calls, schema changes over the last 30 days |
| **Sync Performance** | Rows/minute per sync, average sync duration, per-connector throughput trends |
| **MAR Analytics** | Monthly Active Rows by connector, paid vs free split, monthly trends |
| **Usage History** | Credits/dollars spent, lifetime MAR, monthly usage history table |
| **Schema Changes** | Schema change log with event type and connector breakdown |

## Prerequisites

- Fivetran Platform Connector pointed at a Databricks destination
- [Quickstart dbt transformations](https://fivetran.com/docs/logs/fivetran-platform/quickstart) enabled (produces `fivetran_log_reports` schema)
- Databricks SQL Warehouse

## Usage

1. Edit the constants at the top of the script to match your environment:

```python
HOST          = "your-workspace.azuredatabricks.net"
TOKEN         = "your-databricks-pat"
WAREHOUSE_ID  = "your-warehouse-id"
LOG_REPORTS   = "your_catalog.fivetran_log_reports"
LOG_RAW       = "your_catalog.fivetran_log"
```

2. Run:

```bash
python3 create_fivetran_ops_dashboard.py
```

3. Open the printed URL in your Databricks workspace.

To replace an existing dashboard:

```bash
python3 create_fivetran_ops_dashboard.py --delete <old_dashboard_id>
```

## Notes

- The Sync Performance tab calculates rows/minute by joining `sync_start`, `sync_end`, and `records_modified` events in the raw `fivetran_log.log` table using `sync_id`.
- The `--delete` flag removes the old dashboard before creating a new one (useful for iterating on the design).
