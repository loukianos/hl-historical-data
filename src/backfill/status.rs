use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct BackfillStatus {
    pub state: BackfillState,
    pub started_at: Option<DateTime<Utc>>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub current_hour: Option<String>,
    pub hours_done: i64,
    pub hours_total: i64,
    pub rows_inserted: i64,
    pub rows_quarantined: i64,
    pub files_missing: i64,
    pub files_failed: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackfillState {
    Idle,
    Running,
    Failed,
    Succeeded,
}

impl Default for BackfillStatus {
    fn default() -> Self {
        Self {
            state: BackfillState::Idle,
            started_at: None,
            last_updated_at: None,
            finished_at: None,
            current_hour: None,
            hours_done: 0,
            hours_total: 0,
            rows_inserted: 0,
            rows_quarantined: 0,
            files_missing: 0,
            files_failed: 0,
        }
    }
}

impl BackfillStatus {
    pub fn state_str(&self) -> &'static str {
        match self.state {
            BackfillState::Idle => "idle",
            BackfillState::Running => "running",
            BackfillState::Failed => "failed",
            BackfillState::Succeeded => "succeeded",
        }
    }

    pub fn begin_run(&mut self, now: DateTime<Utc>, hours_total: i64) {
        self.state = BackfillState::Running;
        self.started_at = Some(now);
        self.last_updated_at = Some(now);
        self.finished_at = None;
        self.current_hour = None;
        self.hours_done = 0;
        self.hours_total = hours_total;
        self.rows_inserted = 0;
        self.rows_quarantined = 0;
        self.files_missing = 0;
        self.files_failed = 0;
    }

    /// Update an in-flight snapshot for the current run.
    ///
    /// `hours_done` tracks hours attempted/completed so far (including missing/failed hours).
    /// `current_hour` is the in-flight hour while running and the last attempted hour after finish.
    pub fn update_running(
        &mut self,
        now: DateTime<Utc>,
        current_hour: Option<String>,
        hours_done: i64,
        rows_inserted: i64,
        rows_quarantined: i64,
        files_missing: i64,
        files_failed: i64,
    ) {
        self.state = BackfillState::Running;
        self.last_updated_at = Some(now);
        self.current_hour = current_hour;
        self.hours_done = hours_done;
        self.rows_inserted = rows_inserted;
        self.rows_quarantined = rows_quarantined;
        self.files_missing = files_missing;
        self.files_failed = files_failed;
    }

    pub fn finish(&mut self, now: DateTime<Utc>, state: BackfillState) {
        debug_assert!(matches!(
            state,
            BackfillState::Failed | BackfillState::Succeeded
        ));
        self.state = state;
        self.last_updated_at = Some(now);
        self.finished_at = Some(now);
    }
}

pub type SharedBackfillStatus = Arc<RwLock<BackfillStatus>>;

pub fn new_shared() -> SharedBackfillStatus {
    Arc::new(RwLock::new(BackfillStatus::default()))
}

#[cfg(test)]
mod tests {
    use super::{BackfillState, BackfillStatus};
    use chrono::{DateTime, Duration, Utc};

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn begin_run_resets_counters_and_marks_running() {
        let mut status = BackfillStatus {
            state: BackfillState::Failed,
            started_at: Some(ts("2025-07-27T00:00:00Z")),
            last_updated_at: Some(ts("2025-07-27T00:10:00Z")),
            finished_at: Some(ts("2025-07-27T00:20:00Z")),
            current_hour: Some("20250727/00".to_string()),
            hours_done: 42,
            hours_total: 100,
            rows_inserted: 500,
            rows_quarantined: 9,
            files_missing: 3,
            files_failed: 4,
        };

        let now = ts("2025-07-28T12:00:00Z");
        status.begin_run(now, 12);

        assert_eq!(status.state, BackfillState::Running);
        assert_eq!(status.started_at, Some(now));
        assert_eq!(status.last_updated_at, Some(now));
        assert_eq!(status.finished_at, None);
        assert_eq!(status.current_hour, None);
        assert_eq!(status.hours_done, 0);
        assert_eq!(status.hours_total, 12);
        assert_eq!(status.rows_inserted, 0);
        assert_eq!(status.rows_quarantined, 0);
        assert_eq!(status.files_missing, 0);
        assert_eq!(status.files_failed, 0);
    }

    #[test]
    fn update_running_updates_progress_snapshot() {
        let now = ts("2025-07-28T12:00:00Z");
        let mut status = BackfillStatus::default();
        status.begin_run(now, 10);

        let update_time = now + Duration::minutes(5);
        status.update_running(
            update_time,
            Some("20250728/04".to_string()),
            5,
            123,
            7,
            2,
            1,
        );

        assert_eq!(status.state, BackfillState::Running);
        assert_eq!(status.last_updated_at, Some(update_time));
        assert_eq!(status.current_hour.as_deref(), Some("20250728/04"));
        assert_eq!(status.hours_done, 5);
        assert_eq!(status.hours_total, 10);
        assert_eq!(status.rows_inserted, 123);
        assert_eq!(status.rows_quarantined, 7);
        assert_eq!(status.files_missing, 2);
        assert_eq!(status.files_failed, 1);
    }

    #[test]
    fn finish_marks_terminal_state_and_preserves_current_hour() {
        let now = ts("2025-07-28T12:00:00Z");
        let mut status = BackfillStatus::default();
        status.begin_run(now, 2);
        status.update_running(now, Some("20250728/01".to_string()), 1, 10, 0, 0, 0);

        let finished_at = now + Duration::minutes(1);
        status.finish(finished_at, BackfillState::Failed);

        assert_eq!(status.state, BackfillState::Failed);
        assert_eq!(status.last_updated_at, Some(finished_at));
        assert_eq!(status.finished_at, Some(finished_at));
        assert_eq!(status.current_hour.as_deref(), Some("20250728/01"));
    }
}
