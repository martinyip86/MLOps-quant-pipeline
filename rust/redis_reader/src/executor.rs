use crate::redis_stream::{self, RedisStreamConfig};
use std::sync::Arc;

pub struct RealTimeExecutor {
    client: redis::Client,
    redis_stream_config: RedisStreamConfig,
    streamings: redis_stream::SharedStreamings,
}

impl RealTimeExecutor {
    pub fn new() -> redis::RedisResult<Self> {
        let client = redis::Client::open("redis://127.0.0.1/")?;

        Ok(Self {
            client,
            redis_stream_config: RedisStreamConfig {
                group_name: "executor_data".to_string(),
                consumer_name: "executor_01".to_string(),
                data_types: vec![
                    "orderbook".to_string(),
                    "trades".to_string(),
                    "market_price".to_string(),
                    "open_interest".to_string(),
                ],
            },
            streamings: redis_stream::new_shared_streamings(),
        })
    }

    pub async fn run(self) -> redis::RedisResult<()> {
        // The executor only decides which long-running tasks should run.
        // The Redis details live in redis_stream.rs.
        let refresh_task = tokio::spawn(redis_stream::refresh_stream_keys(
            self.client.clone(),
            self.redis_stream_config.clone(),
            Arc::clone(&self.streamings),
        ));

        let consume_task = tokio::spawn(redis_stream::consume_market_data(
            self.client,
            self.redis_stream_config,
            self.streamings,
        ));

        let (refresh_result, consume_result) = tokio::try_join!(refresh_task, consume_task)
            .expect("executor task panicked");

        refresh_result?;
        consume_result?;

        Ok(())
    }
}
