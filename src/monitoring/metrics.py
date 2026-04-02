from prometheus_client import Gauge,Counter,Histogram,REGISTRY

ws_reconnect_total = Counter(
    "ws_reconnect_total",
    "Websocket reconnect count",
    ["exchange","mkt_type","symbol","method_name"]
)

ws_error_total = Counter(
    "ws_error_total",
    "Websocket error count",
    ["exchange","symbol","mkt_type"]
)

silence_gauge = Gauge(
    "silence_gauge",
    "Silence span",
    ["exchange","symbol","mkt_type","method_name"]
)

redis_mem_gauge = Gauge(
    "redis_memory_gauge",
    "Redis memory monitor",
    ["type"]
)

parquet_write_duration = Histogram(
    "parquet_write_duration_seconds",
    "Parquet duration",
    ["table"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, float("inf"))
)

parquet_write_bytes = Counter(
    "parquet_write_bytes_total",
    "Total number of bytes written as Parquet",
    ["table"]
)