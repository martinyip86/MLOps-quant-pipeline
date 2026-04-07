FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

FROM python:3.12-slim AS final

WORKDIR /app

RUN useradd -m -u 1000 quantuser

COPY --from=builder /install /usr/local
COPY --chown=quantuser:quantuser . .

ENV PATH=/usr/local/bin:$PATH
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV EXCHANGE=binance
ENV TYPE=mkt_type

USER quantuser

CMD ["python", "-m", "src.collectors.manager"]