use anyhow::anyhow;
use serde_json::json;
use std::time::Duration;
use temporal_client::ClientOptionsBuilder;
use temporal_sdk_core::Url;
use workflow_rs::macros::{task, workflow};
use workflow_rs::prelude::*;
use workflow_rs::state::State;
use workflow_rs::task::TaskContext;

#[task(queue = "background-queue")]
#[instrument]
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

#[tokio::main]
async fn main() {
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
        registry.state("EXAMPLE:".to_owned());
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

    // registry.run_worker("background-queue").await.unwrap();

    tracing::info!("Finished running {}", workflow_id);
}
