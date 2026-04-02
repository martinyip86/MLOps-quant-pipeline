import asyncio
from prometheus_client import push_to_gateway,REGISTRY

async def start_metrics_pusher(job_name,url="http://pushgateway:9091"):
    """Background task for Prometheus Pushgateway synchronization."""
    while True:
        try:
            await asyncio.to_thread(
                push_to_gateway,
                url,
                job=job_name,
                registry=REGISTRY
            )
        except: pass
        await asyncio.sleep(10)