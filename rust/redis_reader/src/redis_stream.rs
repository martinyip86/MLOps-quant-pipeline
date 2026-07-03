use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

pub type SharedStreamings = Arc<RwLock<HashMap<String, String>>>;

#[derive(Clone)]
pub struct RedisStreamConfig {
    pub group_name: String,
    pub consumer_name: String,
    pub data_types: Vec<String>,
}

pub fn new_shared_streamings() -> SharedStreamings {
    Arc::new(RwLock::new(HashMap::new()))
}

pub async fn refresh_stream_keys(
    client: redis::Client,
    config: RedisStreamConfig,
    streamings: SharedStreamings,
) -> redis::RedisResult<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;

    loop {
        for data_type in &config.data_types {
            let registry = format!("registry:streams:{data_type}");

            // Redis SMEMBERS returns all stream keys registered by the producer.
            let remote_keys: Vec<String> = match conn.smembers(&registry).await {
                Ok(keys) => keys,
                Err(err) => {
                    eprintln!("smembers error for {registry}: {err}");
                    continue;
                }
            };

            for remote_key in remote_keys {
                // First take a read lock, because most keys are already known.
                // The read lock allows other readers to continue at the same time.
                let already_known = {
                    let streamings_read = streamings.read().await;
                    streamings_read.contains_key(&remote_key)
                };

                if already_known {
                    continue;
                }

                // Python uses mkstream=True. In this crate that is
                // xgroup_create_mkstream, not xgroup_create.
                let create_result: redis::RedisResult<()> = conn
                    .xgroup_create_mkstream(&remote_key, &config.group_name, "0")
                    .await;

                match create_result {
                    Ok(()) => {
                        println!("created group {} for {}", config.group_name, remote_key);
                    }
                    Err(err) => {
                        let message = err.to_string();

                        // BUSYGROUP means the group already exists. That is fine:
                        // this consumer can still read from that group.
                        if !message.contains("BUSYGROUP") {
                            eprintln!("xgroup_create error for {remote_key}: {message}");
                            continue;
                        }
                    }
                }

                // XREADGROUP uses ">" to mean "new messages never delivered
                // to any consumer in this group".
                let mut streamings_write = streamings.write().await;
                streamings_write.insert(remote_key, ">".to_string());
            }
        }

        sleep(Duration::from_secs(30)).await;
    }
}

pub async fn consume_market_data(
    client: redis::Client,
    config: RedisStreamConfig,
    streamings: SharedStreamings,
) -> redis::RedisResult<()> {
    let mut conn = client.get_multiplexed_async_connection().await?;

    loop {
        // Copy the current stream list, then release the lock before the Redis
        // blocking read. Holding the lock during BLOCK 2000 would prevent the
        // refresh task from adding new streams promptly.
        let (keys, ids): (Vec<String>, Vec<String>) = {
            let streamings_read = streamings.read().await;

            if streamings_read.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let keys = streamings_read.keys().cloned().collect();
                let ids = streamings_read.values().cloned().collect();
                (keys, ids)
            }
        };

        if keys.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let options = redis::streams::StreamReadOptions::default()
            .group(&config.group_name, &config.consumer_name)
            .count(500)
            .block(2000);

        let response: redis::RedisResult<redis::streams::StreamReadReply> =
            conn.xread_options(&keys, &ids, &options).await;

        let response = match response {
            Ok(response) => response,
            Err(err) => {
                eprintln!("xreadgroup error: {err}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        for stream in response.keys {
            for message in stream.ids {
                if let Some(data) = message.map.get("data") {
                    println!("{} | {} | {:?}", stream.key, message.id, data);
                } else {
                    println!("{} | {} | no data field", stream.key, message.id);
                }

                // Acknowledge only after the message has been handled.
                // Later, when you add feature/strategy logic, put xack after
                // that logic succeeds.
                let ack_result: redis::RedisResult<usize> =
                    conn.xack(&stream.key, &config.group_name, &[&message.id]).await;

                if let Err(err) = ack_result {
                    eprintln!("xack error for {} {}: {}", stream.key, message.id, err);
                }
            }
        }
    }
}
