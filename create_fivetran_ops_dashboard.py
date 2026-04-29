"""
Creates a Fivetran Operations Lakeview dashboard in Databricks.
Uses chrisrudolph.fivetran_log_reports (Platform Connector quickstart transformations)
plus chrisrudolph.fivetran_log for raw sync performance metrics.

Usage:
    python3 create_fivetran_ops_dashboard.py [--delete <dashboard_id>]
"""

import json
import os
import sys
import requests

HOST         = os.environ.get("DATABRICKS_HOST",         "adb-3043026989721781.1.azuredatabricks.net")
TOKEN        = os.environ.get("DATABRICKS_TOKEN",        "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "aafaeaff93adefe4")
LOG_REPORTS  = os.environ.get("FIVETRAN_LOG_REPORTS",    "chrisrudolph.fivetran_log_reports")
LOG_RAW      = os.environ.get("FIVETRAN_LOG_RAW",        "chrisrudolph.fivetran_log")

if not TOKEN:
    sys.exit("Error: set DATABRICKS_TOKEN environment variable")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Dataset IDs
# ---------------------------------------------------------------------------
DS_STATUS = "ds_conn_status"
DS_DAILY  = "ds_daily_events"
DS_MAR    = "ds_mar_history"
DS_USAGE  = "ds_usage"
DS_SCHEMA = "ds_schema_chg"
DS_PERF   = "ds_sync_perf"


def sql_lines(query: str) -> list[str]:
    lines = query.strip().split("\n")
    return [l + "\n" if i < len(lines) - 1 else l for i, l in enumerate(lines)]


DATASETS = [
    {
        "name": DS_STATUS,
        "displayName": "connector_status",
        "queryLines": sql_lines(
            "SELECT\n"
            "  *,\n"
            "  1                                                                     AS connector_count,\n"
            "  CASE WHEN connection_health = 'connected'                  THEN 1 ELSE 0 END AS is_healthy,\n"
            "  CASE WHEN connection_health NOT IN ('connected', 'paused') THEN 1 ELSE 0 END AS has_issues\n"
            f"FROM {LOG_REPORTS}.fivetran_platform__connection_status"
        ),
    },
    {
        "name": DS_DAILY,
        "displayName": "connection_daily_events",
        "queryLines": sql_lines(
            f"SELECT *\n"
            f"FROM {LOG_REPORTS}.fivetran_platform__connection_daily_events\n"
            "WHERE date_day >= CURRENT_DATE - INTERVAL 30 DAYS"
        ),
    },
    {
        "name": DS_MAR,
        "displayName": "mar_table_history",
        "queryLines": sql_lines(
            "SELECT\n"
            "  connection_name,\n"
            "  connector_type,\n"
            "  destination_name,\n"
            "  measured_month,\n"
            "  SUM(total_monthly_active_rows) AS total_mar,\n"
            "  SUM(paid_monthly_active_rows)  AS paid_mar,\n"
            "  SUM(free_monthly_active_rows)  AS free_mar\n"
            f"FROM {LOG_REPORTS}.fivetran_platform__mar_table_history\n"
            "GROUP BY 1,2,3,4"
        ),
    },
    {
        "name": DS_USAGE,
        "displayName": "usage_history",
        "queryLines": sql_lines(
            f"SELECT *\n"
            f"FROM {LOG_REPORTS}.fivetran_platform__usage_history\n"
            "ORDER BY measured_month DESC"
        ),
    },
    {
        "name": DS_SCHEMA,
        "displayName": "schema_changelog",
        "queryLines": sql_lines(
            "SELECT\n"
            "  created_at,\n"
            "  connection_name,\n"
            "  destination_name,\n"
            "  event_subtype,\n"
            "  schema_name,\n"
            "  table_name,\n"
            "  message_data\n"
            f"FROM {LOG_REPORTS}.fivetran_platform__schema_changelog\n"
            "ORDER BY created_at DESC\n"
            "LIMIT 500"
        ),
    },
    {
        "name": DS_PERF,
        "displayName": "sync_performance",
        "queryLines": sql_lines(
            "WITH sync_times AS (\n"
            "  SELECT\n"
            "    sync_id,\n"
            "    connection_id,\n"
            "    MIN(CASE WHEN message_event = 'sync_start' THEN time_stamp END) AS sync_started_at,\n"
            "    MAX(CASE WHEN message_event = 'sync_end'   THEN time_stamp END) AS sync_ended_at,\n"
            "    MAX(CASE WHEN message_event = 'sync_end'\n"
            "             THEN get_json_object(message_data, '$.status') END)    AS sync_status\n"
            f"  FROM {LOG_RAW}.log\n"
            "  WHERE message_event IN ('sync_start', 'sync_end')\n"
            "  GROUP BY sync_id, connection_id\n"
            "),\n"
            "sync_rows AS (\n"
            "  SELECT\n"
            "    sync_id,\n"
            "    SUM(CAST(get_json_object(message_data, '$.count') AS BIGINT)) AS total_rows\n"
            f"  FROM {LOG_RAW}.log\n"
            "  WHERE message_event = 'records_modified'\n"
            "  GROUP BY sync_id\n"
            ")\n"
            "SELECT\n"
            "  t.sync_id,\n"
            "  t.connection_id,\n"
            "  c.connection_name,\n"
            "  c.connector_type,\n"
            "  t.sync_started_at,\n"
            "  t.sync_ended_at,\n"
            "  t.sync_status,\n"
            "  COALESCE(r.total_rows, 0)                                           AS total_rows,\n"
            "  ROUND((unix_timestamp(t.sync_ended_at)\n"
            "       - unix_timestamp(t.sync_started_at)) / 60.0, 2)               AS duration_minutes,\n"
            "  CASE\n"
            "    WHEN (unix_timestamp(t.sync_ended_at) - unix_timestamp(t.sync_started_at)) > 0\n"
            "    THEN ROUND(COALESCE(r.total_rows, 0)\n"
            "               / ((unix_timestamp(t.sync_ended_at)\n"
            "                 - unix_timestamp(t.sync_started_at)) / 60.0))\n"
            "    ELSE 0\n"
            "  END AS rows_per_minute\n"
            "FROM sync_times t\n"
            f"LEFT JOIN sync_rows r ON t.sync_id = r.sync_id\n"
            f"LEFT JOIN {LOG_REPORTS}.fivetran_platform__connection_status c\n"
            "       ON t.connection_id = c.connection_id\n"
            "WHERE t.sync_started_at IS NOT NULL\n"
            "  AND t.sync_ended_at   IS NOT NULL\n"
            "  AND t.sync_started_at >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS\n"
            "ORDER BY t.sync_started_at DESC"
        ),
    },
]


# ---------------------------------------------------------------------------
# Widget builders
# ---------------------------------------------------------------------------

def counter(wid, ds, field_expr, field_label, title):
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds,
                "fields": [{"name": field_label, "expression": field_expr}],
                "disaggregated": False,
            }}],
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {
                    "value": {"fieldName": field_label, "displayName": title}
                },
                "frame": {"showTitle": True, "title": title},
            },
        }
    }


def bar(wid, ds, x_field, x_expr, x_label, y_field, y_expr, y_label, title=None):
    spec = {
        "version": 3,
        "widgetType": "bar",
        "encodings": {
            "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_label},
            "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
        },
    }
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds,
                "fields": [
                    {"name": x_field, "expression": x_expr},
                    {"name": y_field, "expression": y_expr},
                ],
                "disaggregated": False,
            }}],
            "spec": spec,
        }
    }


def line(wid, ds, x_field, x_expr, y_field, y_expr, y_label,
         color_field=None, color_expr=None, title=None):
    fields = [
        {"name": x_field, "expression": x_expr},
        {"name": y_field, "expression": y_expr},
    ]
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": x_field},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
    }
    if color_field and color_expr:
        fields.append({"name": color_field, "expression": color_expr})
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    spec = {"version": 3, "widgetType": "line", "encodings": encodings}
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds,
                "fields": fields,
                "disaggregated": True,
            }}],
            "spec": spec,
        }
    }


def table(wid, ds, columns: list[tuple], title=None):
    """columns: list of (field_name, expression, display_name)"""
    fields = [{"name": n, "expression": e} for n, e, _ in columns]
    col_specs = [{"fieldName": n, "displayName": d} for n, _, d in columns]
    spec = {
        "version": 2,
        "widgetType": "table",
        "encodings": {"columns": col_specs},
    }
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "widget": {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": ds,
                "fields": fields,
                "disaggregated": True,
            }}],
            "spec": spec,
        }
    }


def place(widget_def, x, y, w, h):
    widget_def["position"] = {"x": x, "y": y, "width": w, "height": h}
    return widget_def


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_overview():
    widgets = [
        place(counter("ov_total",
                      DS_STATUS,
                      "SUM(`connector_count`)",
                      "total_connectors",
                      "Connectors Tracked"),
              0, 0, 2, 3),
        place(counter("ov_healthy",
                      DS_STATUS,
                      "SUM(`is_healthy`)",
                      "healthy",
                      "Healthy Connectors"),
              2, 0, 2, 3),
        place(counter("ov_issues",
                      DS_STATUS,
                      "SUM(`has_issues`)",
                      "with_issues",
                      "Connectors With Issues"),
              4, 0, 2, 3),
        place(bar("ov_health_bar",
                  DS_STATUS,
                  "connection_health", "`connection_health`", "Health Status",
                  "connector_count",   "COUNT(`connection_id`)", "Connectors",
                  title="Connectors by Health Status"),
              0, 3, 3, 6),
        place(bar("ov_sync_bar",
                  DS_STATUS,
                  "connection_name", "`connection_name`", "Connector",
                  "errors",
                  "SUM(`number_errors_since_last_completed_sync`)",
                  "Errors Since Last Sync",
                  title="Errors Since Last Completed Sync"),
              3, 3, 3, 6),
        place(table("ov_status_tbl", DS_STATUS, [
            ("connection_name",                       "`connection_name`",                               "Connector"),
            ("connector_type",                        "`connector_type`",                                "Type"),
            ("connection_health",                     "`connection_health`",                             "Health"),
            ("last_successful_sync_completed_at",     "`last_successful_sync_completed_at`",             "Last Successful Sync"),
            ("number_errors_since_last_completed_sync",   "`number_errors_since_last_completed_sync`",   "Errors"),
            ("number_warnings_since_last_completed_sync", "`number_warnings_since_last_completed_sync`", "Warnings"),
        ], title="Connector Status Detail"),
              0, 9, 6, 8),
    ]
    return {
        "name": "page_overview",
        "displayName": "Operations Overview",
        "layout": widgets,
        "pageType": "CANVAS",
    }


def page_connector_health():
    widgets = [
        place(counter("ch_rec_mods",
                      DS_DAILY,
                      "SUM(`count_record_modifications`)",
                      "record_mods_30d",
                      "Record Modifications (30d)"),
              0, 0, 2, 3),
        place(counter("ch_api_calls",
                      DS_DAILY,
                      "SUM(`count_api_calls`)",
                      "api_calls_30d",
                      "API Calls (30d)"),
              2, 0, 2, 3),
        place(counter("ch_schema_chg",
                      DS_DAILY,
                      "SUM(`count_schema_changes`)",
                      "schema_chg_30d",
                      "Schema Changes (30d)"),
              4, 0, 2, 3),
        place(line("ch_rec_trend",
                   DS_DAILY,
                   "date_day", "`date_day`",
                   "record_modifications",
                   "SUM(`count_record_modifications`)",
                   "Record Modifications",
                   color_field="connection_name",
                   color_expr="`connection_name`",
                   title="Daily Record Modifications by Connector (Last 30 Days)"),
              0, 3, 6, 6),
        place(bar("ch_api_bar",
                  DS_DAILY,
                  "connection_name", "`connection_name`", "Connector",
                  "api_calls",       "SUM(`count_api_calls`)", "API Calls",
                  title="Total API Calls by Connector (Last 30 Days)"),
              0, 9, 6, 5),
    ]
    return {
        "name": "page_connector_health",
        "displayName": "Connector Health",
        "layout": widgets,
        "pageType": "CANVAS",
    }


def page_sync_performance():
    widgets = [
        place(counter("sp_avg_rpm",
                      DS_PERF,
                      "AVG(`rows_per_minute`)",
                      "avg_rows_per_min",
                      "Avg Rows/Min (All Connectors)"),
              0, 0, 2, 3),
        place(counter("sp_total_rows",
                      DS_PERF,
                      "SUM(`total_rows`)",
                      "total_rows_30d",
                      "Total Rows Synced (30d)"),
              2, 0, 2, 3),
        place(counter("sp_avg_dur",
                      DS_PERF,
                      "AVG(`duration_minutes`)",
                      "avg_duration_min",
                      "Avg Sync Duration (min)"),
              4, 0, 2, 3),
        place(bar("sp_rpm_bar",
                  DS_PERF,
                  "connection_name", "`connection_name`", "Connector",
                  "rows_per_minute", "AVG(`rows_per_minute`)", "Avg Rows/Min",
                  title="Average Rows/Minute by Connector"),
              0, 3, 3, 6),
        place(bar("sp_dur_bar",
                  DS_PERF,
                  "connection_name", "`connection_name`", "Connector",
                  "duration_minutes", "AVG(`duration_minutes`)", "Avg Duration (min)",
                  title="Average Sync Duration by Connector"),
              3, 3, 3, 6),
        place(line("sp_rpm_trend",
                   DS_PERF,
                   "sync_started_at", "`sync_started_at`",
                   "rows_per_minute", "`rows_per_minute`", "Rows/Min",
                   color_field="connection_name",
                   color_expr="`connection_name`",
                   title="Rows/Minute per Sync (Last 30 Days)"),
              0, 9, 6, 6),
        place(table("sp_tbl", DS_PERF, [
            ("sync_started_at",   "`sync_started_at`",   "Started At"),
            ("connection_name",   "`connection_name`",   "Connector"),
            ("sync_status",       "`sync_status`",       "Status"),
            ("total_rows",        "`total_rows`",        "Rows Synced"),
            ("duration_minutes",  "`duration_minutes`",  "Duration (min)"),
            ("rows_per_minute",   "`rows_per_minute`",   "Rows/Min"),
        ], title="Sync History (Last 30 Days)"),
              0, 15, 6, 8),
    ]
    return {
        "name": "page_sync_perf",
        "displayName": "Sync Performance",
        "layout": widgets,
        "pageType": "CANVAS",
    }


def page_mar():
    widgets = [
        place(counter("mar_total", DS_MAR, "SUM(`total_mar`)", "total_mar", "Total MAR"),
              0, 0, 2, 3),
        place(counter("mar_paid",  DS_MAR, "SUM(`paid_mar`)",  "paid_mar",  "Paid MAR"),
              2, 0, 2, 3),
        place(counter("mar_free",  DS_MAR, "SUM(`free_mar`)",  "free_mar",  "Free MAR"),
              4, 0, 2, 3),
        place(bar("mar_by_conn",
                  DS_MAR,
                  "connection_name", "`connection_name`", "Connector",
                  "total_mar",       "SUM(`total_mar`)",  "Total MAR",
                  title="Total MAR by Connector"),
              0, 3, 6, 6),
        place(line("mar_monthly",
                   DS_MAR,
                   "measured_month", "`measured_month`",
                   "total_mar", "SUM(`total_mar`)", "Total MAR",
                   color_field="connection_name",
                   color_expr="`connection_name`",
                   title="Monthly MAR Trend by Connector"),
              0, 9, 6, 6),
    ]
    return {
        "name": "page_mar",
        "displayName": "MAR Analytics",
        "layout": widgets,
        "pageType": "CANVAS",
    }


def page_usage():
    widgets = [
        place(counter("us_total_mar", DS_USAGE, "SUM(`total_monthly_active_rows`)", "total_mar",  "Lifetime MAR"),
              0, 0, 2, 3),
        place(counter("us_credits",   DS_USAGE, "SUM(`credits_spent`)",             "credits",    "Total Credits Spent"),
              2, 0, 2, 3),
        place(counter("us_dollars",   DS_USAGE, "SUM(`dollars_spent`)",             "dollars",    "Total Dollars Spent"),
              4, 0, 2, 3),
        place(line("us_mar_trend",
                   DS_USAGE,
                   "measured_month", "`measured_month`",
                   "total_mar", "SUM(`total_monthly_active_rows`)", "Total MAR",
                   title="Monthly MAR Trend"),
              0, 3, 6, 6),
        place(table("us_tbl", DS_USAGE, [
            ("measured_month",            "`measured_month`",                "Month"),
            ("destination_name",          "`destination_name`",              "Destination"),
            ("total_monthly_active_rows", "`total_monthly_active_rows`",     "Total MAR"),
            ("paid_monthly_active_rows",  "`paid_monthly_active_rows`",      "Paid MAR"),
            ("credits_spent",             "`credits_spent`",                 "Credits Spent"),
            ("dollars_spent",             "`dollars_spent`",                 "Dollars Spent"),
        ], title="Monthly Usage History"),
              0, 9, 6, 8),
    ]
    return {
        "name": "page_usage",
        "displayName": "Usage History",
        "layout": widgets,
        "pageType": "CANVAS",
    }


def page_schema_changes():
    widgets = [
        place(counter("sc_total", DS_SCHEMA, "COUNT(1)", "total_changes", "Schema Changes (Last 500)"),
              0, 0, 3, 3),
        place(bar("sc_by_type",
                  DS_SCHEMA,
                  "event_subtype", "`event_subtype`", "Event Type",
                  "change_count",  "COUNT(1)",         "Count",
                  title="Schema Changes by Event Type"),
              0, 3, 3, 5),
        place(bar("sc_by_conn",
                  DS_SCHEMA,
                  "connection_name", "`connection_name`", "Connector",
                  "change_count",    "COUNT(1)",           "Count",
                  title="Schema Changes by Connector"),
              3, 3, 3, 5),
        place(table("sc_tbl", DS_SCHEMA, [
            ("created_at",      "`created_at`",      "Timestamp"),
            ("connection_name", "`connection_name`", "Connector"),
            ("event_subtype",   "`event_subtype`",   "Event Type"),
            ("schema_name",     "`schema_name`",     "Schema"),
            ("table_name",      "`table_name`",      "Table"),
            ("message_data",    "`message_data`",    "Details"),
        ], title="Schema Change Log"),
              0, 8, 6, 10),
    ]
    return {
        "name": "page_schema_changes",
        "displayName": "Schema Changes",
        "layout": widgets,
        "pageType": "CANVAS",
    }


# ---------------------------------------------------------------------------
# Create / publish
# ---------------------------------------------------------------------------

def delete_dashboard(dash_id: str):
    resp = requests.delete(
        f"https://{HOST}/api/2.0/lakeview/dashboards/{dash_id}",
        headers=HEADERS,
    )
    if resp.status_code not in (200, 204):
        print(f"Delete warning {resp.status_code}: {resp.text}", file=sys.stderr)
    else:
        print(f"Deleted dashboard {dash_id}")


def create_dashboard(display_name: str = "Fivetran Operations - Wesco POC") -> str:
    dashboard_def = {
        "datasets": DATASETS,
        "pages": [
            page_overview(),
            page_connector_health(),
            page_sync_performance(),
            page_mar(),
            page_usage(),
            page_schema_changes(),
        ],
    }
    payload = {
        "display_name": display_name,
        "warehouse_id": WAREHOUSE_ID,
        "serialized_dashboard": json.dumps(dashboard_def),
    }
    resp = requests.post(
        f"https://{HOST}/api/2.0/lakeview/dashboards",
        headers=HEADERS,
        json=payload,
    )
    if resp.status_code not in (200, 201):
        print(f"ERROR {resp.status_code}: {resp.text}", file=sys.stderr)
        sys.exit(1)
    dash_id = resp.json()["dashboard_id"]
    print(f"Dashboard created: {dash_id}")
    return dash_id


def publish_dashboard(dash_id: str):
    resp = requests.post(
        f"https://{HOST}/api/2.0/lakeview/dashboards/{dash_id}/published",
        headers=HEADERS,
        json={"warehouse_id": WAREHOUSE_ID},
    )
    if resp.status_code not in (200, 201):
        print(f"Publish warning {resp.status_code}: {resp.text}", file=sys.stderr)
    else:
        print(f"URL: https://{HOST}/dashboardsv3/{dash_id}?o=3043026989721781")


if __name__ == "__main__":
    if "--delete" in sys.argv:
        idx = sys.argv.index("--delete")
        delete_dashboard(sys.argv[idx + 1])

    dash_id = create_dashboard()
    publish_dashboard(dash_id)
