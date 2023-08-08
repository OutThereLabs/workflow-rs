use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone)]
pub struct WorkflowInstanceID {
    pub id: String,
    pub workflow_name: &'static str,
}

#[derive(Clone)]
pub struct InMemoryExecutorStorage {
    pub results: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl Default for InMemoryExecutorStorage {
    fn default() -> Self {
        Self {
            results: Arc::new(Mutex::new(Default::default())),
        }
    }
}
