use redis::AsyncCommands;
use std::collections::HashMap;

struct RealTimeExecutor {
    conn: redis::aio::MultiplexedConnection,
    group_name: String,
    consumer_name: String,
    streamings: HashMap<String,String>,
}

impl RealTimeExecutor {
    async fn new()->Self{
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();

        let conn = client.get_multiplexed_async_connection().await.unwrap();

        Self{
            conn,
            group_name:"executor_data".into(),
            consumer_name:"executor01".into(),
            streamings:HashMap::new()
        }
    }
}

#[tokio::main]
async fn main(){
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    loop{
        let res: redis::streams::StreamReadReply = conn.xread_options(&["market:btc"],&[">"],&redis::streams::StreamReadOptions::default().group("executor_data","executor01").block(2000).count(500)).await.unwrap();

        if let Some(reply)=res{
            for stream in reply.keys{
                for msg in stream.ids{
                    let data=msg.map.get("data");

                    if let Some(v)=data{
                        let s:String=redis::from_redis_value(v).unwrap();
                        println!("{}",s);
                    }
                }
            }
        }
    }
}

