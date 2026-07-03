mod executor;
mod redis_stream;

use executor::RealTimeExecutor;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let executor = RealTimeExecutor::new()?;
    executor.run().await
}
