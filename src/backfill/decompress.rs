use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HourFiles {
    pub lz4_path: PathBuf,
    pub jsonl_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecompressStats {
    pub decompressed_bytes: u64,
}

pub fn hour_files(temp_dir: &str, key: &str) -> Result<HourFiles> {
    let lz4_path = Path::new(temp_dir).join(key);

    if lz4_path.extension().and_then(|ext| ext.to_str()) != Some("lz4") {
        bail!("expected hourly key to end with .lz4: {key}");
    }

    let jsonl_path = lz4_path.with_extension("jsonl");
    Ok(HourFiles {
        lz4_path,
        jsonl_path,
    })
}

pub fn decompress_lz4_to_jsonl(input_lz4: &Path, output_jsonl: &Path) -> Result<DecompressStats> {
    let partial_output = partial_output_path(output_jsonl);

    let result = (|| -> Result<DecompressStats> {
        if let Some(parent) = output_jsonl.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create output directory {}", parent.display())
                })?;
            }
        }

        let input_file = File::open(input_lz4)
            .with_context(|| format!("failed to open input file {}", input_lz4.display()))?;
        let reader = BufReader::new(input_file);

        let mut decoder = lz4::Decoder::new(reader).with_context(|| {
            format!(
                "failed to initialize lz4 decoder for {}",
                input_lz4.display()
            )
        })?;

        let partial_file = File::create(&partial_output).with_context(|| {
            format!(
                "failed to create temporary output file {}",
                partial_output.display()
            )
        })?;
        let mut writer = BufWriter::new(partial_file);

        let decompressed_bytes = std::io::copy(&mut decoder, &mut writer).with_context(|| {
            format!(
                "failed to decompress {} into {}",
                input_lz4.display(),
                partial_output.display()
            )
        })?;

        writer
            .flush()
            .with_context(|| format!("failed to flush output file {}", partial_output.display()))?;

        let (_, finish_result) = decoder.finish();
        finish_result.with_context(|| {
            format!(
                "lz4 decoder reported trailing error for {}",
                input_lz4.display()
            )
        })?;

        drop(writer);

        match std::fs::remove_file(output_jsonl) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to replace existing output file {}",
                        output_jsonl.display()
                    )
                });
            }
        }

        std::fs::rename(&partial_output, output_jsonl).with_context(|| {
            format!(
                "failed to finalize decompressed output {}",
                output_jsonl.display()
            )
        })?;

        Ok(DecompressStats { decompressed_bytes })
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&partial_output);
    }

    result
}

fn partial_output_path(output_jsonl: &Path) -> PathBuf {
    let mut partial = output_jsonl.as_os_str().to_os_string();
    partial.push(".partial");
    PathBuf::from(partial)
}

#[cfg(test)]
mod tests {
    use super::{decompress_lz4_to_jsonl, hour_files, partial_output_path};
    use lz4::EncoderBuilder;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_temp_dir(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        path.push(format!("hl_historical_{label}_{suffix}"));
        fs::create_dir_all(&path).expect("failed to create temp dir");
        path
    }

    fn write_lz4(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create lz4 parent directory");
        }

        let file = fs::File::create(path).expect("failed to create lz4 file");
        let mut encoder = EncoderBuilder::new()
            .build(file)
            .expect("failed to build lz4 encoder");

        encoder
            .write_all(bytes)
            .expect("failed to write compressed bytes");

        let (_, result) = encoder.finish();
        result.expect("failed to finish lz4 encoding");
    }

    #[test]
    fn hour_files_builds_expected_paths() {
        let files = hour_files(
            "/tmp/hl-backfill",
            "node_fills_by_block/hourly/20250727/00.lz4",
        )
        .expect("hourly key should map to temp files");

        assert_eq!(
            files.lz4_path,
            PathBuf::from("/tmp/hl-backfill/node_fills_by_block/hourly/20250727/00.lz4")
        );
        assert_eq!(
            files.jsonl_path,
            PathBuf::from("/tmp/hl-backfill/node_fills_by_block/hourly/20250727/00.jsonl")
        );
    }

    #[test]
    fn hour_files_rejects_non_lz4_keys() {
        let err = hour_files(
            "/tmp/hl-backfill",
            "node_fills_by_block/hourly/20250727/00.gz",
        )
        .expect_err("non-lz4 key should fail");

        assert!(err.to_string().contains("end with .lz4"));
    }

    #[test]
    fn decompress_round_trip_writes_expected_jsonl() {
        let temp_dir = create_temp_dir("decompress_round_trip");
        let input_lz4 = temp_dir.join("node_fills_by_block/hourly/20250727/00.lz4");
        let output_jsonl = temp_dir.join("node_fills_by_block/hourly/20250727/00.jsonl");
        let payload = b"{\"coin\":\"BTC\"}\n{\"coin\":\"ETH\"}\n";

        write_lz4(&input_lz4, payload);

        let stats = decompress_lz4_to_jsonl(&input_lz4, &output_jsonl)
            .expect("lz4 decompression should succeed");
        let output = fs::read(&output_jsonl).expect("failed to read decompressed output");

        assert_eq!(output, payload);
        assert_eq!(stats.decompressed_bytes, payload.len() as u64);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn decompress_replaces_existing_output_file() {
        let temp_dir = create_temp_dir("decompress_replaces_output");
        let input_lz4 = temp_dir.join("node_fills_by_block/hourly/20250727/01.lz4");
        let output_jsonl = temp_dir.join("node_fills_by_block/hourly/20250727/01.jsonl");
        let payload = b"{\"coin\":\"SOL\"}\n";

        write_lz4(&input_lz4, payload);

        if let Some(parent) = output_jsonl.parent() {
            fs::create_dir_all(parent).expect("failed to create output parent dir");
        }
        fs::write(&output_jsonl, b"stale-data").expect("failed to seed old output file");

        decompress_lz4_to_jsonl(&input_lz4, &output_jsonl)
            .expect("decompression should replace existing output");
        let output = fs::read(&output_jsonl).expect("failed to read replaced output");

        assert_eq!(output, payload);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn decompress_corrupt_input_cleans_partial_file() {
        let temp_dir = create_temp_dir("decompress_corrupt");
        let input_lz4 = temp_dir.join("node_fills_by_block/hourly/20250727/02.lz4");
        let output_jsonl = temp_dir.join("node_fills_by_block/hourly/20250727/02.jsonl");

        if let Some(parent) = input_lz4.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dir");
        }

        fs::write(&input_lz4, b"not-a-valid-lz4-frame").expect("failed to write corrupt lz4 input");

        let err = decompress_lz4_to_jsonl(&input_lz4, &output_jsonl)
            .expect_err("corrupt lz4 input should fail decompression");
        let partial = partial_output_path(&output_jsonl);

        assert!(
            err.to_string().contains("failed to decompress") || err.to_string().contains("decoder")
        );
        assert!(!output_jsonl.exists());
        assert!(!partial.exists());

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn decompress_large_stream_reports_expected_byte_count() {
        let temp_dir = create_temp_dir("decompress_large_stream");
        let input_lz4 = temp_dir.join("node_fills_by_block/hourly/20250727/02.lz4");
        let output_jsonl = temp_dir.join("node_fills_by_block/hourly/20250727/02.jsonl");

        if let Some(parent) = input_lz4.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dir");
        }

        let file = fs::File::create(&input_lz4).expect("failed to create lz4 file");
        let mut encoder = EncoderBuilder::new()
            .build(file)
            .expect("failed to build lz4 encoder");

        let mut expected_bytes = 0_u64;
        for i in 0..25_000_u32 {
            let line = format!("{{\"i\":{},\"coin\":\"BTC\",\"px\":\"100.0\"}}\n", i);
            expected_bytes += line.len() as u64;
            encoder
                .write_all(line.as_bytes())
                .expect("failed to write encoded line");
        }

        let (_, result) = encoder.finish();
        result.expect("failed to finalize large lz4 file");

        let stats = decompress_lz4_to_jsonl(&input_lz4, &output_jsonl)
            .expect("large stream decompression should succeed");
        let output_size = fs::metadata(&output_jsonl)
            .expect("failed to stat output file")
            .len();

        assert_eq!(stats.decompressed_bytes, expected_bytes);
        assert_eq!(output_size, expected_bytes);

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
