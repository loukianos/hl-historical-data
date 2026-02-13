use anyhow::{bail, Context, Result};
use clap::Parser;
use prost_types::Timestamp;
use std::time::Instant;

mod proto {
    tonic::include_proto!("hl_historical");
}

#[derive(Debug, Parser)]
#[command(
    name = "bench-time-bars",
    about = "Benchmark GetTimeBars query latency and throughput"
)]
struct Args {
    /// gRPC endpoint for HistoricalDataService.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,

    /// Coin symbol (example: BTC).
    #[arg(long)]
    coin: String,

    /// Interval: 1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 1d.
    #[arg(long)]
    interval: String,

    /// Start time (unix seconds).
    #[arg(long)]
    start: i64,

    /// End time (unix seconds, exclusive).
    #[arg(long)]
    end: i64,
}

fn parse_interval(value: &str) -> Option<i32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1s" => Some(proto::Interval::Interval1s as i32),
        "5s" => Some(proto::Interval::Interval5s as i32),
        "30s" => Some(proto::Interval::Interval30s as i32),
        "1m" => Some(proto::Interval::Interval1m as i32),
        "5m" => Some(proto::Interval::Interval5m as i32),
        "15m" => Some(proto::Interval::Interval15m as i32),
        "30m" => Some(proto::Interval::Interval30m as i32),
        "1h" => Some(proto::Interval::Interval1h as i32),
        "4h" => Some(proto::Interval::Interval4h as i32),
        "1d" => Some(proto::Interval::Interval1d as i32),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let coin = args.coin.trim();

    if coin.is_empty() {
        bail!("--coin cannot be empty");
    }
    if args.start >= args.end {
        bail!("--start must be strictly earlier than --end");
    }

    let interval = parse_interval(&args.interval).with_context(|| {
        format!(
            "unsupported interval '{}'; expected one of 1s/5s/30s/1m/5m/15m/30m/1h/4h/1d",
            args.interval
        )
    })?;

    let mut client =
        proto::historical_data_service_client::HistoricalDataServiceClient::connect(args.addr)
            .await
            .context("failed to connect to HistoricalDataService")?;

    let request = proto::GetTimeBarsRequest {
        coin: coin.to_string(),
        interval,
        start_time: Some(Timestamp {
            seconds: args.start,
            nanos: 0,
        }),
        end_time: Some(Timestamp {
            seconds: args.end,
            nanos: 0,
        }),
    };

    let started = Instant::now();
    let mut stream = client
        .get_time_bars(request)
        .await
        .context("GetTimeBars RPC failed")?
        .into_inner();

    let mut bars = 0_u64;
    let mut total_notional = 0.0_f64;
    while let Some(resp) = stream
        .message()
        .await
        .context("GetTimeBars stream read failed")?
    {
        if let Some(bar) = resp.bar {
            bars += 1;
            total_notional += bar.notional_volume;
        }
    }

    let elapsed = started.elapsed();
    let bars_per_sec = if elapsed.as_secs_f64() > 0.0 {
        bars as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("GetTimeBars benchmark complete");
    println!("  coin: {}", coin);
    println!("  interval: {}", args.interval);
    println!("  start: {}", args.start);
    println!("  end: {}", args.end);
    println!("  bars returned: {}", bars);
    println!("  total notional: {:.6}", total_notional);
    println!("  elapsed: {:.3}s", elapsed.as_secs_f64());
    println!("  throughput: {:.2} bars/s", bars_per_sec);

    Ok(())
}
