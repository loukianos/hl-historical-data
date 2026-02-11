use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct BackfillStatus {
    pub state: BackfillState,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
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
}

pub type SharedBackfillStatus = Arc<RwLock<BackfillStatus>>;

pub fn new_shared() -> SharedBackfillStatus {
    Arc::new(RwLock::new(BackfillStatus::default()))
}
