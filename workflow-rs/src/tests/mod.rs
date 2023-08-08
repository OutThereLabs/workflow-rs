use crate::macros::{task, workflow};
use crate::registry::WorkflowRegistry;
use crate::state::State;
use crate::task::TaskContext;
use crate::workflow::WorkflowContext;
use crate::{self as workflow_rs};
use anyhow::anyhow;
use serde_json::json;
use std::time::Duration;
use temporal_client::ClientOptionsBuilder;
use temporal_sdk_core::Url;

#[task(queue = "background-queue")]
async fn echo(
    context: TaskContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let prefix: State<String> = context.state()?;

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

#[tokio::test]
async fn test_in_memory_workflow() {
    let registry = WorkflowRegistry::new_in_memory(|registry| {
        registry.register_workflow(&multi_line_echo);
        registry.register_task(&echo);
        registry.state("TEST".to_owned());
    });

    let workflow_id = format!("test-workflow-{}", uuid::Uuid::new_v4());

    registry
        .clone()
        .start_workflow(
            workflow_id,
            &multi_line_echo,
            vec![json!("Hello"), json!("World")],
        )
        .await
        .expect("Could not start workflow");

    registry
        .clone()
        .run_worker("background-queue")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_temporal_workflow() {
    let client = ClientOptionsBuilder::default()
        .target_url(Url::parse("http://localhost:7233").unwrap())
        .client_name("test_temporal_workflow")
        .client_version("0.1")
        .build()
        .unwrap()
        .connect("default", None, None)
        .await
        .unwrap();
    let registry = WorkflowRegistry::new_temporal(client, |registry| {
        registry.register_workflow(&multi_line_echo);
        registry.register_task(&echo);
        registry.state("TEST".to_owned());
    });

    let workflow_id = format!("test-workflow-{}", uuid::Uuid::new_v4());

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
        .run_until_done(workflow_id, "background-queue", Duration::from_secs(5))
        .await
        .unwrap();
}
