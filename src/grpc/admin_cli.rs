use crate::config::Config;
use crate::grpc::proto;
use anyhow::{anyhow, Result};
use chrono::{TimeZone, Utc};
use prost_types::Timestamp;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

const CONNECT_TIMEOUT_SECONDS: u64 = 3;
const PARTITION_PRINT_LIMIT: usize = 100;
const PARTITION_PREVIEW_EDGE: usize = 10;

type AdminClient =
    proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient<Channel>;

pub async fn run(action: crate::AdminAction, config: &Config) -> Result<()> {
    let (addr, normalization_note) = build_admin_addr(config);
    let normalization_note = normalization_note.as_deref();

    let mut client = connect_admin_client(&addr)
        .await
        .map_err(|err| map_connect_error(&addr, normalization_note, err))?;

    match action {
        crate::AdminAction::Status => {
            let status = client
                .get_ingestion_status(proto::GetIngestionStatusRequest {})
                .await
                .map_err(|status| {
                    map_rpc_status(&addr, normalization_note, "GetIngestionStatus", status)
                })?
                .into_inner();

            println!("{}", render_ingestion_status(&status));
        }
        crate::AdminAction::Sync => {
            let response = client
                .sync_to_present(proto::SyncToPresentRequest {})
                .await
                .map_err(|status| {
                    map_rpc_status(&addr, normalization_note, "SyncToPresent", status)
                })?
                .into_inner();

            println!(
                "{}",
                render_acceptance_response(response.accepted, &response.message)
            );
        }
        crate::AdminAction::Backfill { from, to } => {
            let response = client
                .trigger_backfill(proto::TriggerBackfillRequest {
                    from_date: from,
                    to_date: to,
                })
                .await
                .map_err(|status| {
                    map_rpc_status(&addr, normalization_note, "TriggerBackfill", status)
                })?
                .into_inner();

            println!(
                "{}",
                render_acceptance_response(response.accepted, &response.message)
            );
        }
        crate::AdminAction::Stats => {
            let stats = client
                .get_db_stats(proto::GetDbStatsRequest {})
                .await
                .map_err(|status| map_rpc_status(&addr, normalization_note, "GetDbStats", status))?
                .into_inner();

            println!("{}", render_db_stats(&stats));
        }
        crate::AdminAction::Purge { before } => {
            let response = client
                .purge_data(proto::PurgeDataRequest {
                    before_date: before,
                })
                .await
                .map_err(|status| map_rpc_status(&addr, normalization_note, "PurgeData", status))?
                .into_inner();

            println!("{}", render_purge_response(&response));
        }
        crate::AdminAction::Reindex => {
            let response = client
                .re_index(proto::ReIndexRequest {})
                .await
                .map_err(|status| map_rpc_status(&addr, normalization_note, "ReIndex", status))?
                .into_inner();

            println!("{}", render_message_response(&response.message));
        }
    }

    Ok(())
}

fn build_admin_addr(config: &Config) -> (String, Option<String>) {
    let (normalized_host, note) = normalize_admin_host(&config.grpc.host);
    let host_for_uri = format_host_for_uri(&normalized_host);
    let addr = format!("http://{}:{}", host_for_uri, config.grpc.port);
    (addr, note)
}

fn normalize_admin_host(host: &str) -> (String, Option<String>) {
    let host = host.trim();
    match host {
        "0.0.0.0" => (
            "127.0.0.1".to_string(),
            Some("grpc.host=0.0.0.0 is a bind address; admin CLI will dial 127.0.0.1".to_string()),
        ),
        "::" | "[::]" => (
            "::1".to_string(),
            Some("grpc.host=:: is a bind address; admin CLI will dial ::1".to_string()),
        ),
        _ => (host.to_string(), None),
    }
}

fn format_host_for_uri(host: &str) -> String {
    if host.starts_with('[') && host.ends_with(']') {
        host.to_string()
    } else if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

async fn connect_admin_client(addr: &str) -> Result<AdminClient> {
    let endpoint = Endpoint::from_shared(addr.to_string())
        .map_err(|err| anyhow!("Invalid gRPC admin endpoint '{addr}': {err}"))?
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECONDS));

    let channel = endpoint.connect().await?;
    Ok(AdminClient::new(channel))
}

fn map_connect_error(
    addr: &str,
    normalization_note: Option<&str>,
    err: anyhow::Error,
) -> anyhow::Error {
    let mut message = format!(
        "Could not reach admin gRPC server at {addr}.\nStart it first with `hl-historical-data serve --config <config.toml>` and verify grpc.host/grpc.port in your config."
    );

    if let Some(note) = normalization_note {
        message.push_str(&format!("\nNote: {note}"));
    }

    anyhow!("{message}\nCause: {err}")
}

fn map_rpc_status(
    addr: &str,
    normalization_note: Option<&str>,
    operation: &'static str,
    status: Status,
) -> anyhow::Error {
    match status.code() {
        Code::Unavailable => {
            let mut message = format!(
                "{operation} failed: service unavailable at {addr}."
            );

            if !status.message().trim().is_empty() {
                message.push_str(&format!("\nServer message: {}", status.message()));
            }

            if let Some(note) = normalization_note {
                message.push_str(&format!("\nNote: {note}"));
            }

            message.push_str(
                "\nIf the gRPC server is not running, start it with `hl-historical-data serve --config <config.toml>` and verify grpc.host/grpc.port."
            );

            anyhow!(message)
        }
        Code::Unimplemented => anyhow!(
            "{operation} failed: connected to {addr}, but HistoricalDataAdminService is not exposed there."
        ),
        Code::InvalidArgument | Code::FailedPrecondition => {
            anyhow!("{operation} failed: {}", format_status_message(&status))
        }
        _ => anyhow!(
            "{operation} failed ({:?}): {}",
            status.code(),
            format_status_message(&status)
        ),
    }
}

fn format_status_message(status: &Status) -> String {
    let message = status.message().trim();
    if message.is_empty() {
        "(no server message)".to_string()
    } else {
        message.to_string()
    }
}

fn render_ingestion_status(status: &proto::GetIngestionStatusResponse) -> String {
    let mut lines = vec![format!("State: {}", status.state)];

    if let Some(timestamp) = status.started_at.as_ref() {
        lines.push(format!("Started at (UTC): {}", fmt_timestamp(timestamp)));
    }

    if let Some(timestamp) = status.last_updated_at.as_ref() {
        lines.push(format!("Last updated (UTC): {}", fmt_timestamp(timestamp)));
    }

    if let Some(timestamp) = status.finished_at.as_ref() {
        lines.push(format!("Finished at (UTC): {}", fmt_timestamp(timestamp)));
    }

    let current_hour = status.current_hour.trim();
    if !current_hour.is_empty() {
        lines.push(format!("Current hour: {current_hour}"));
    }

    lines.push(format!(
        "Progress: {}/{} hours",
        status.hours_done, status.hours_total
    ));
    lines.push(format!("Rows inserted: {}", status.rows_inserted));
    lines.push(format!("Rows quarantined: {}", status.rows_quarantined));
    lines.push(format!("Files missing: {}", status.files_missing));
    lines.push(format!("Files failed: {}", status.files_failed));

    lines.join("\n")
}

fn render_acceptance_response(accepted: bool, message: &str) -> String {
    let accepted = if accepted { "yes" } else { "no" };
    format!(
        "Accepted: {accepted}\nMessage: {}",
        format_optional_message(message)
    )
}

fn render_purge_response(response: &proto::PurgeDataResponse) -> String {
    format!(
        "Partitions dropped: {}\nMessage: {}",
        response.partitions_dropped,
        format_optional_message(&response.message)
    )
}

fn render_message_response(message: &str) -> String {
    format!("Message: {}", format_optional_message(message))
}

fn render_db_stats(stats: &proto::GetDbStatsResponse) -> String {
    let mut lines = vec![
        format!("Fills rows: {}", stats.fills_count),
        format!("Quarantine rows: {}", stats.quarantine_count),
        format!(
            "Min fill time (UTC): {}",
            fmt_optional_timestamp(stats.min_time.as_ref())
        ),
        format!(
            "Max fill time (UTC): {}",
            fmt_optional_timestamp(stats.max_time.as_ref())
        ),
    ];

    let mut partitions = stats.partitions.clone();
    partitions.sort_by(|left, right| left.date.cmp(&right.date));

    lines.push(format!("Partitions: {}", partitions.len()));

    if partitions.is_empty() {
        lines.push("Partition row counts: none reported".to_string());
        return lines.join("\n");
    }

    lines.push("Partition row counts:".to_string());

    if partitions.len() <= PARTITION_PRINT_LIMIT {
        for partition in &partitions {
            lines.push(format!("  {} -> {}", partition.date, partition.row_count));
        }
        return lines.join("\n");
    }

    for partition in partitions.iter().take(PARTITION_PREVIEW_EDGE) {
        lines.push(format!("  {} -> {}", partition.date, partition.row_count));
    }

    let omitted_count = partitions.len().saturating_sub(PARTITION_PREVIEW_EDGE * 2);
    lines.push(format!("  ... omitted {omitted_count} partitions ..."));

    for partition in partitions
        .iter()
        .skip(partitions.len().saturating_sub(PARTITION_PREVIEW_EDGE))
    {
        lines.push(format!("  {} -> {}", partition.date, partition.row_count));
    }

    lines.join("\n")
}

fn fmt_optional_timestamp(timestamp: Option<&Timestamp>) -> String {
    timestamp
        .map(fmt_timestamp)
        .unwrap_or_else(|| "n/a".to_string())
}

fn fmt_timestamp(timestamp: &Timestamp) -> String {
    if !(0..=999_999_999).contains(&timestamp.nanos) {
        return format!("seconds={} nanos={}", timestamp.seconds, timestamp.nanos);
    }

    match Utc
        .timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
        .single()
    {
        Some(datetime) => datetime.to_rfc3339(),
        None => format!("seconds={} nanos={}", timestamp.seconds, timestamp.nanos),
    }
}

fn format_optional_message(message: &str) -> String {
    let message = message.trim();
    if message.is_empty() {
        "(no message)".to_string()
    } else {
        message.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn config_with_grpc_host(host: &str, port: u16) -> Config {
        let mut config = Config::default();
        config.grpc.host = host.to_string();
        config.grpc.port = port;
        config
    }

    #[test]
    fn build_admin_addr_normalizes_ipv4_wildcard_host() {
        let config = config_with_grpc_host("0.0.0.0", 50051);

        let (addr, note) = build_admin_addr(&config);

        assert_eq!(addr, "http://127.0.0.1:50051");
        assert!(note
            .expect("expected normalization note")
            .contains("0.0.0.0"));
    }

    #[test]
    fn build_admin_addr_formats_ipv6_hosts() {
        let config = config_with_grpc_host("::1", 50051);

        let (addr, note) = build_admin_addr(&config);

        assert_eq!(addr, "http://[::1]:50051");
        assert!(note.is_none());
    }

    #[test]
    fn render_ingestion_status_includes_progress_and_counters() {
        let response = proto::GetIngestionStatusResponse {
            state: "running".to_string(),
            started_at: Some(Timestamp {
                seconds: 1_735_689_600,
                nanos: 0,
            }),
            last_updated_at: None,
            finished_at: None,
            current_hour: "20251014-08".to_string(),
            hours_done: 3,
            hours_total: 10,
            rows_inserted: 120,
            rows_quarantined: 5,
            files_missing: 2,
            files_failed: 1,
        };

        let rendered = render_ingestion_status(&response);

        assert!(rendered.contains("State: running"));
        assert!(rendered.contains("Current hour: 20251014-08"));
        assert!(rendered.contains("Progress: 3/10 hours"));
        assert!(rendered.contains("Rows inserted: 120"));
        assert!(rendered.contains("Rows quarantined: 5"));
        assert!(rendered.contains("Files missing: 2"));
        assert!(rendered.contains("Files failed: 1"));
    }

    #[test]
    fn render_db_stats_truncates_large_partition_output() {
        let partitions = (1..=120)
            .map(|day| proto::PartitionInfo {
                date: format!("2025{:04}", day),
                row_count: day as i64,
            })
            .collect();

        let stats = proto::GetDbStatsResponse {
            fills_count: 10,
            quarantine_count: 2,
            min_time: None,
            max_time: None,
            partitions,
        };

        let rendered = render_db_stats(&stats);

        assert!(rendered.contains("Partitions: 120"));
        assert!(rendered.contains("... omitted 100 partitions ..."));
        assert!(rendered.contains("20250001 -> 1"));
        assert!(rendered.contains("20250120 -> 120"));
    }

    #[test]
    fn map_rpc_status_unavailable_includes_recovery_guidance() {
        let error = map_rpc_status(
            "http://127.0.0.1:50051",
            None,
            "GetDbStats",
            Status::unavailable("connection refused"),
        );

        let message = error.to_string();

        assert!(message.contains("unavailable"));
        assert!(message.contains("serve --config"));
        assert!(message.contains("connection refused"));
    }

    #[test]
    fn fmt_timestamp_falls_back_for_invalid_nanos() {
        let rendered = fmt_timestamp(&Timestamp {
            seconds: 123,
            nanos: -1,
        });

        assert_eq!(rendered, "seconds=123 nanos=-1");
    }
}
