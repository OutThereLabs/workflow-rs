use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use futures_util::stream::FuturesUnordered;
use futures_util::stream::StreamExt;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct WorkflowInstanceID {
    pub id: String,
    pub workflow_name: &'static str,
}

#[derive(Clone)]
pub struct InMemoryExecutorStorage {
    pub tasks: Arc<Mutex<Vec<JoinHandle<anyhow::Result<()>>>>>,
    pub results: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl Default for InMemoryExecutorStorage {
    fn default() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(vec![])),
            results: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl InMemoryExecutorStorage {
    pub fn add_task(&self, handle: JoinHandle<anyhow::Result<()>>) {
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.push(handle);
        }
    }

    pub async fn execute_all(&self) -> anyhow::Result<()> {
        let handles = self
            .tasks
            .lock()
            .ok()
            .map(|mut t| mem::take(&mut *t))
            .unwrap_or_default();

        let mut futures = handles.into_iter().collect::<FuturesUnordered<_>>();

        while let Some(r) = futures.next().await {
            let _ = r?;
        }

        Ok(())
    }
}
