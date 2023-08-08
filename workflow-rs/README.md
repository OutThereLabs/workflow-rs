# workflow-rs

`workflow-rs` is a workflow engine client that focuses on simple workflows that can be executed against a variety of backends.

Inspired by [Active Job](https://guides.rubyonrails.org/active_job_basics.html) and [sqlxmq](https://github.com/Diggsey/sqlxmq).

## Examples

### Scheduling Workflows

```rust
#[task(queue = "background-queue")]
#[instrument]
async fn echo(
    context: TaskContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let prefix: State<String> = context.state()?;
    let string = input.as_str().unwrap_or_default();
    tracing::info!("{prefix} {string}");
    Ok(serde_json::Value::String(format!("{prefix} {string}")))
}

#[workflow(queue = "background-queue")]
async fn multi_line_echo(
    context: WorkflowContext,
    args: Vec<serde_json::Value>,
) -> Result<serde_json::Value, anyhow::Error> {
    for arg in args {
        context.task(echo, arg.clone()).await?;
    }
    Ok(serde_json::Value::Null)
}

#[tokio::main]
async fn main() {
    let registry = WorkflowRegistry::from_env(|registry| {
        registry.register_workflow(&multi_line_echo);
        registry.register_task(&echo);
        registry.state("EXAMPLE:".to_owned());
    })
    .unwrap();

    let workflow_id = format!("main-workflow-{}", uuid::Uuid::new_v4());

    registry
        .start_workflow(
            workflow_id.clone(),
            &multi_line_echo,
            vec![json!("Hello"), json!("World")],
        )
        .await
        .expect("Could not start workflow");
}
```

## High level features

### Done
- [x] Basic `#[workflow]` and `#[task]` macros 
- [x] In memory engine that can be used for testing, no external dependencies
- [x] Temporal.io client
- [x] Timers (sleep)

### TODO Soon
- [ ] Extractors (`Data<T>` and `State<T>`) and a macro to make that easy
- [ ] Workflow results (this might require rewriting the temporal SDK wrapper)
- [ ] `#[job]` macro that turns a task into a simple workflow

### TODO Later
- [ ] [Conditions](https://typescript.temporal.io/api/namespaces/workflow)
- [ ] AWS SWF client (see the [AWS Flow Framework](https://docs.aws.amazon.com/amazonswf/latest/awsflowguide/getting-started-example-helloworldworkflow.html))
 