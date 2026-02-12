#[derive(Debug, Clone, Default)]
pub struct BackfillRunReport {
    pub from: String,
    pub to: String,
    pub hours_total: usize,
    pub hours_processed: usize,
    pub hours_missing: usize,
    pub failed_download: usize,
    pub failed_decompress: usize,
    pub failed_dedup: usize,
    pub failed_ingest: usize,
    pub rows_inserted: i64,
    pub rows_quarantined: i64,
    pub parse_errors: i64,
}

impl BackfillRunReport {
    pub fn new(from: &str, to: &str, hours_total: usize) -> Self {
        Self {
            from: from.to_owned(),
            to: to.to_owned(),
            hours_total,
            ..Self::default()
        }
    }

    pub fn failed_hours(&self) -> usize {
        self.failed_download + self.failed_decompress + self.failed_dedup + self.failed_ingest
    }

    pub fn hours_skipped(&self) -> usize {
        self.hours_missing + self.failed_hours()
    }

    pub fn render(&self, keep_temp_files: bool) -> String {
        format!(
            "Backfill summary\n  range (inclusive dates): {}..={}\n  hours total: {}\n  hours processed: {}\n  hours skipped: {}\n  rows inserted: {}\n  rows quarantined: {}\n  files missing: {}\n  failed download: {}\n  failed decompress: {}\n  failed dedup: {}\n  failed ingest: {}\n  parse errors: {}\n  keep temp files: {}",
            self.from,
            self.to,
            self.hours_total,
            self.hours_processed,
            self.hours_skipped(),
            self.rows_inserted,
            self.rows_quarantined,
            self.hours_missing,
            self.failed_download,
            self.failed_decompress,
            self.failed_dedup,
            self.failed_ingest,
            self.parse_errors,
            keep_temp_files,
        )
    }
}
