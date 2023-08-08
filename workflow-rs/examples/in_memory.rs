use anyhow::anyhow;
use serde_json::json;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use workflow_rs::macros::{task, workflow};
use workflow_rs::prelude::*;
use workflow_rs::state::State;
use workflow_rs::task::TaskContext;

struct PrefixProvider {
    prefix: &'static str,
}

impl Display for PrefixProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.prefix)
    }
}

#[task(queue = "background-queue")]
#[instrument]
async fn echo(
    context: TaskContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let prefix: State<PrefixProvider> = context.state()?;

    if let Some(string) = input.as_str() {
        tracing::info!("{prefix} {string}");
        Ok(serde_json::Value::String(string.to_owned()))
    } else {
        Err(anyhow!("Error, non string input"))
    }
}

#[workflow(queue = "background-queue")]
async fn multi_line_echo(
    context: WorkflowContext,
    args: Vec<serde_json::Value>,
) -> Result<serde_json::Value, anyhow::Error> {
    for arg in args {
        let _: String = context.task(echo, arg.clone()).await?;
    }
    Ok(serde_json::Value::Null)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let registry = WorkflowRegistry::new_in_memory(|registry| {
        registry.register_workflow(&multi_line_echo);
        registry.register_task(&echo);
        registry.state(PrefixProvider { prefix: "EXAMPLE:" });
    });

    let workflow_id = format!("main-workflow-{}", uuid::Uuid::new_v4());

    registry
        .clone()
        .start_workflow(
            workflow_id.clone(),
            &multi_line_echo,
            vec![json!("Hello"), json!("World")],
        )
        .await
        .expect("Could not start workflow");

    registry
        .clone()
        .run_until_done(
            workflow_id.clone(),
            "background-queue",
            Duration::from_secs(10),
        )
        .await
        .expect("Could not run work");

    tracing::info!("Finished running {}", workflow_id);
}
