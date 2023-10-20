use crate::in_memory::InMemoryExecutorStorage;
use crate::registry::WorkflowRegistry;
use crate::state::Extensions;

use crate::task::TaskContext;
use crate::task::TaskFactory;
use crate::task::TemporalTaskAdapter;
use anyhow::{anyhow, Error};

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use temporal_client::WorkflowClientTrait;
use temporal_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, WfContext, Worker,
};
use temporal_sdk_core::protos::coresdk::activity_result::activity_resolution;
use temporal_sdk_core::protos::coresdk::{activity_result, AsJsonPayloadExt};
use temporal_sdk_core::protos::coresdk::{
    child_workflow::child_workflow_result::Status as ChildWorkflowResultStatus,
    workflow_activation::resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
};
use temporal_sdk_core::protos::temporal::api::common::v1::RetryPolicy as TemporalRetryPolicy;
use temporal_sdk_core_api::telemetry::{
    Logger, MetricsExporter, OtelCollectorOptions, TraceExporter,
};
use uuid::Uuid;

#[derive(Clone)]
pub enum WorkflowWorker {
    InMemory(InMemoryExecutorStorage),
    #[cfg(feature = "temporal")]
    Temporal {
        extensions: Arc<Extensions>,
        client: temporal_client::RetryClient<temporal_client::Client>,
        queue: &'static str,
        build_id: &'static str,
    },
}

impl WorkflowWorker {
    pub async fn run(
        self,
        workflow_factories: HashMap<&'static str, &'static dyn WorkflowFactory>,
        task_factories: HashMap<&'static str, &'static dyn TaskFactory>,
    ) -> Result<(), Error> {
        match self.clone() {
            Self::InMemory(_) => Ok(()),
            #[cfg(feature = "temporal")]
            Self::Temporal {
                extensions,
                client,
                queue,
                build_id,
            } => {
                use temporal_sdk_core::init_worker;
                use temporal_sdk_core::CoreRuntime;
                use temporal_sdk_core::WorkerConfigBuilder;
                use temporal_sdk_core_api::telemetry::TelemetryOptionsBuilder;
                use temporal_sdk_core_api::telemetry::TraceExportConfig;

                let log_level = std::env::var("RUST_LOG").unwrap_or("info".to_owned());

                let telemetry_options = TelemetryOptionsBuilder::default()
                    .tracing(TraceExportConfig {
                        filter: log_level.clone(),
                        exporter: TraceExporter::Otel(OtelCollectorOptions {
                            url: "grpc://localhost:4317".parse().unwrap(),
                            headers: Default::default(),
                            metric_periodicity: None,
                        }),
                    })
                    .metrics(MetricsExporter::Otel(OtelCollectorOptions {
                        url: "grpc://localhost:4317".parse().unwrap(),
                        headers: Default::default(),
                        metric_periodicity: None,
                    }))
                    .logging(Logger::Console { filter: log_level })
                    .build()?;
                let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;

                let worker_config = WorkerConfigBuilder::default()
                    .task_queue(queue.to_owned())
                    .namespace(client.namespace())
                    .worker_build_id(build_id.to_owned())
                    .build()?;

                let core_worker = init_worker(&runtime, worker_config, client.clone())?;

                let mut worker = Worker::new_from_core(Arc::new(core_worker), queue.to_owned());

                worker.insert_app_data(extensions.clone());

                for (name, factory) in workflow_factories {
                    worker.register_wf(
                        name.to_owned(),
                        TemporalWorkflowAdapter {
                            workflow: factory.builder(vec![]).handler,
                            worker: self.clone(),
                        },
                    )
                }

                for (name, factory) in task_factories {
                    worker.register_activity(name.to_owned(), TemporalTaskAdapter(factory))
                }

                worker.run().await?;
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub enum TaskLauncher {
    InMemory(Arc<WorkflowRegistry>),
    #[cfg(feature = "temporal")]
    Temporal {
        context: Arc<WfContext>,
        worker: WorkflowWorker,
    },
}

pub struct RetryPolicy {
    pub initial_interval: Duration,
    pub multiplier: f64,
    pub max_attempts: Option<u32>,
}

impl Into<backoff::ExponentialBackoff> for RetryPolicy {
    fn into(self) -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            initial_interval: self.initial_interval,
            multiplier: self.multiplier,
            max_elapsed_time: self
                .max_attempts
                .map(|max_retries| self.initial_interval * max_retries),
            ..Default::default()
        }
    }
}

#[cfg(feature = "temporal")]
impl Into<TemporalRetryPolicy> for RetryPolicy {
    fn into(self) -> TemporalRetryPolicy {
        TemporalRetryPolicy {
            initial_interval: Some(prost_wkt_types::Duration {
                nanos: self.initial_interval.subsec_nanos() as i32,
                seconds: self.initial_interval.as_secs() as i64,
            }),
            backoff_coefficient: self.multiplier,
            maximum_attempts: self
                .max_attempts
                .map(|max_retries| max_retries as i32)
                .unwrap_or_default(),
            ..Default::default()
        }
    }
}

#[derive(Clone)]
pub struct WorkflowContext {
    pub workflow_id: String,
    task_launcher: TaskLauncher,
}

impl<'a> WorkflowContext {
    pub fn new(workflow_id: String, task_launcher: TaskLauncher) -> Self {
        Self {
            workflow_id,
            task_launcher,
        }
    }

    pub async fn task<F: TaskFactory, I: serde::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        factory: F,
        input: I,
    ) -> Result<O, Error> {
        let value = self
            .task_by_name(
                factory.name(),
                factory.queue_name(),
                serde_json::json!(input),
            )
            .await?;
        Ok(serde_json::from_value(value).map_err(|error| {
            tracing::warn!("Could not encode value: {}", error);
            error
        })?)
    }

    pub async fn task_with_explicit_retry<
        F: TaskFactory,
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
        R: Into<RetryPolicy>,
    >(
        &self,
        factory: F,
        input: I,
        retry_policy: R,
    ) -> Result<O, Error> {
        let value = self
            .task_by_name_with_explicit_retry(
                factory.name(),
                factory.queue_name(),
                serde_json::json!(input),
                retry_policy,
            )
            .await?;
        Ok(serde_json::from_value(value).map_err(|error| {
            tracing::warn!("Could not encode value: {}", error);
            error
        })?)
    }

    pub async fn local_task<F: TaskFactory, I: serde::Serialize>(
        &self,
        factory: F,
        input: I,
    ) -> Result<serde_json::Value, Error> {
        self.local_task_by_name(factory.name(), serde_json::json!(input))
            .await
    }

    pub async fn child_workflow<D: ToString, F: WorkflowFactory, I: serde::Serialize>(
        &self,
        id: D,
        factory: F,
        input: Vec<I>,
    ) -> Result<(), Error> {
        let input_json = input.into_iter().map(|i| serde_json::json!(i)).collect();
        self.child_workflow_by_name(id, factory.name(), input_json)
            .await
    }

    pub async fn task_by_name(
        &self,
        name: &'static str,
        queue: &'static str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        match &self.task_launcher {
            TaskLauncher::InMemory(registry) => {
                Self::in_memory_task(name, input, registry.clone()).await
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context, worker: _ } => {
                Self::temporal_task(name, queue, input, context).await
            }
        }
    }

    pub async fn task_by_name_with_explicit_retry<R: Into<RetryPolicy>>(
        &self,
        name: &'static str,
        queue: &'static str,
        input: Value,
        retry_policy: R,
    ) -> Result<serde_json::Value, Error> {
        match &self.task_launcher {
            TaskLauncher::InMemory(registry) => {
                Self::in_memory_task_with_explicit_retry(
                    name,
                    input,
                    registry.clone(),
                    retry_policy.into(),
                )
                .await
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context, worker: _ } => {
                Self::temporal_task_with_explicit_retry(
                    name,
                    queue,
                    input,
                    context,
                    retry_policy.into(),
                )
                .await
            }
        }
    }

    pub async fn local_task_by_name(
        &self,
        name: &'static str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        match &self.task_launcher {
            TaskLauncher::InMemory(registry) => {
                Self::in_memory_task(name, input, registry.clone()).await
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context, worker: _ } => {
                Self::temporal_local_task(name, input, context).await
            }
        }
    }

    pub async fn workflow_by_name<T: ToString>(
        self,
        id: T,
        name: &'static str,
        queue: &'static str,
        input: Vec<Value>,
    ) -> Result<(), Error> {
        match self.task_launcher {
            TaskLauncher::InMemory(registry) => {
                Self::in_memory_workflow(id, name, input, registry.clone()).await
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context: _, worker } => match worker {
                WorkflowWorker::InMemory(_) => {
                    panic!("Invalid worker")
                }
                WorkflowWorker::Temporal {
                    extensions: _,
                    client,
                    queue: _queue,
                    build_id: _,
                } => {
                    let _ = client
                        .start_workflow(
                            input
                                .iter()
                                .map(|value| value.as_json_payload().unwrap())
                                .collect(),
                            queue.to_string(),
                            id.to_string(),
                            name.to_owned(),
                            None,
                            Default::default(),
                        )
                        .await?;

                    Ok(())
                }
            },
        }
    }

    pub async fn child_workflow_by_name<T: ToString>(
        &self,
        id: T,
        name: &'static str,
        input: Vec<Value>,
    ) -> Result<(), Error> {
        match &self.task_launcher {
            TaskLauncher::InMemory(registry) => {
                Self::in_memory_workflow(id, name, input, registry.clone()).await
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context, worker: _ } => {
                Self::temporal_child_workflow(id, name, input, context).await
            }
        }
    }

    pub async fn sleep(&self, duration: Duration) {
        match &self.task_launcher {
            TaskLauncher::InMemory(_) => {
                tokio::time::sleep(duration).await;
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal { context, worker: _ } => {
                let _ = context.timer(duration).await;
            }
        }
    }

    async fn in_memory_task(
        name: &'static str,
        input: Value,
        registry: Arc<WorkflowRegistry>,
    ) -> Result<Value, Error> {
        match registry.task_factories.get(name) {
            Some(task) => {
                let task_context = TaskContext {
                    extensions: registry.extensions.clone(),
                };
                let task = task.builder(input);
                task.call(task_context).await
            }
            None => Err(anyhow!("No task registed for {name}")),
        }
    }

    async fn in_memory_task_with_explicit_retry(
        name: &'static str,
        input: Value,
        registry: Arc<WorkflowRegistry>,
        retry_policy: RetryPolicy,
    ) -> Result<Value, Error> {
        match registry.task_factories.get(name) {
            Some(task) => {
                let backoff: backoff::ExponentialBackoff = retry_policy.into();
                backoff::future::retry(backoff, || async {
                    let task_context = TaskContext {
                        extensions: registry.extensions.clone(),
                    };
                    let task = task.builder(input.clone());
                    Ok(task.call(task_context).await?)
                })
                .await
            }
            None => Err(anyhow!("No task registed for {name}")),
        }
    }

    async fn in_memory_workflow<T: ToString>(
        id: T,
        name: &'static str,
        input: Vec<Value>,
        registry: Arc<WorkflowRegistry>,
    ) -> Result<(), Error> {
        match registry.workflow_factories.get(name) {
            Some(workflow) => {
                registry
                    .executor
                    .clone()
                    .start_workflow(
                        id.to_string(),
                        workflow.name(),
                        workflow.queue_name(),
                        input,
                        registry,
                    )
                    .await?;
                Ok(())
            }
            None => Err(anyhow!("No task registed for {name}")),
        }
    }

    #[cfg(feature = "temporal")]
    async fn temporal_task(
        name: &'static str,
        queue: &'static str,
        input: Value,
        context: &'a WfContext,
    ) -> Result<Value, Error> {
        let activity_options = ActivityOptions {
            input: input.as_json_payload()?,
            activity_type: name.to_owned(),
            task_queue: queue.to_owned(),
            start_to_close_timeout: Some(Duration::from_secs(600)),
            schedule_to_start_timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        };
        let result = context.activity(activity_options).await;

        match result
            .status
            .expect("Unexpected null on activity result status")
        {
            activity_resolution::Status::Completed(activity_result::Success {
                result: Some(payload),
            }) => {
                let value = serde_json::from_slice(payload.data.as_slice()).unwrap();
                Ok(value)
            }
            other => Err(anyhow!("Task failed: {other:#?}")),
        }
    }

    #[cfg(feature = "temporal")]
    async fn temporal_task_with_explicit_retry(
        name: &'static str,
        queue: &'static str,
        input: Value,
        context: &'a WfContext,
        retry_policy: RetryPolicy,
    ) -> Result<Value, Error> {
        let activity_options = ActivityOptions {
            input: input.as_json_payload()?,
            activity_type: name.to_owned(),
            task_queue: queue.to_owned(),
            start_to_close_timeout: Some(Duration::from_secs(600)),
            schedule_to_start_timeout: Some(Duration::from_secs(600)),
            retry_policy: Some(retry_policy.into()),
            ..Default::default()
        };
        let result = context.activity(activity_options).await;

        match result
            .status
            .expect("Unexpected null on activity result status")
        {
            activity_resolution::Status::Completed(activity_result::Success {
                result: Some(payload),
            }) => {
                let value = serde_json::from_slice(payload.data.as_slice()).unwrap();
                Ok(value)
            }
            other => Err(anyhow!("Task failed: {other:#?}")),
        }
    }

    #[cfg(feature = "temporal")]
    async fn temporal_local_task(
        name: &'static str,
        input: Value,
        context: &'a WfContext,
    ) -> Result<Value, Error> {
        let activity_options = LocalActivityOptions {
            input: input.as_json_payload()?,
            activity_type: name.to_owned(),
            start_to_close_timeout: Some(Duration::from_secs(600)),
            schedule_to_start_timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        };
        let result = context.local_activity(activity_options).await;

        match result
            .status
            .expect("Unexpected null on activity result status")
        {
            activity_resolution::Status::Completed(activity_result::Success {
                result: Some(payload),
            }) => {
                let value = serde_json::from_slice(payload.data.as_slice()).unwrap();
                Ok(value)
            }
            other => Err(anyhow!("Task failed: {other:#?}")),
        }
    }

    #[cfg(feature = "temporal")]
    async fn temporal_child_workflow<I: ToString>(
        workflow_id: I,
        name: &'static str,
        input: Vec<Value>,
        context: &'a WfContext,
    ) -> Result<(), Error> {
        let mut payloads = vec![];
        for item in input {
            payloads.push(item.as_json_payload()?);
        }

        let child_workflow_options = ChildWorkflowOptions {
            workflow_id: workflow_id.to_string(),
            workflow_type: name.to_string(),
            input: payloads,
            ..Default::default()
        };
        let result = context
            .child_workflow(child_workflow_options)
            .start(context)
            .await;

        match result.status.clone() {
            ChildWorkflowStartStatus::Succeeded(_s) => {
                let future = result.into_started().unwrap();
                let result = future.result().await;

                match result.status {
                    Some(status) => match status {
                        ChildWorkflowResultStatus::Completed(result) => {
                            tracing::debug!("Result: {result:?}");
                            Ok(())
                        }
                        ChildWorkflowResultStatus::Failed(error) => {
                            Err(anyhow!("Failed: {error:?}"))
                        }
                        ChildWorkflowResultStatus::Cancelled(error) => {
                            Err(anyhow!("Cancelled: {error:?}"))
                        }
                    },
                    None => Err(anyhow!("Could not submit child workflow")),
                }
            }
            ChildWorkflowStartStatus::Failed(error) => {
                let cause = error.cause;
                Err(anyhow!("Failed to start child workflow: {cause}"))
            }
            ChildWorkflowStartStatus::Cancelled(error) => {
                if let Some(failure) = error.failure {
                    let message = failure.message;
                    Err(anyhow!("Child workflow start cancelled: {message}"))
                } else {
                    Err(anyhow!("Child workflow start cancelled"))
                }
            }
        }
    }
}

pub trait WorkflowHandler {
    fn call(
        &self,
        context: WorkflowContext,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, anyhow::Error>> + Unpin>>;
}

type PinBoxJsonFuture = Pin<Box<dyn Future<Output = Result<serde_json::Value, Error>> + Send>>;

#[derive(Clone)]
pub struct WorkflowRunFunction(pub fn(WorkflowContext, Vec<serde_json::Value>) -> PinBoxJsonFuture);

pub struct WorkflowHandlerFunction<
    F: Future<Output = Result<serde_json::Value, anyhow::Error>>,
    FN: Fn(WorkflowContext) -> F,
>(pub Box<FN>);

impl<
        F: Future<Output = Result<serde_json::Value, anyhow::Error>> + 'static + Unpin,
        FN: Fn(WorkflowContext) -> F,
    > WorkflowHandler for WorkflowHandlerFunction<F, FN>
{
    fn call(
        &self,
        context: WorkflowContext,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, Error>> + Unpin>> {
        let future = (*self.0)(context);
        Box::pin(future)
    }
}

impl WorkflowHandler
    for fn(
        WorkflowContext,
    ) -> Box<dyn Future<Output = Result<serde_json::Value, Error>> + 'static + Unpin>
{
    fn call(
        &self,
        context: WorkflowContext,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, Error>> + Unpin>> {
        Box::pin(self(context))
    }
}

#[derive(Clone)]
pub struct Workflow {
    pub id: String,
    pub workflow_name: &'static str,
    pub queue: &'static str,
    pub args: Vec<serde_json::Value>,
    pub handler: WorkflowRunFunction,
}

impl Workflow {
    pub async fn execute<'a>(&self, context: WorkflowContext) -> Result<(), anyhow::Error> {
        let _ = &self.handler.0(context, self.args.clone()).await?;

        Ok(())
    }
}

pub trait WorkflowFactory: Sync + Send {
    fn name(&self) -> &'static str;
    fn queue_name(&self) -> &'static str;
    fn builder(&self, input: Vec<serde_json::Value>) -> Workflow;
}

#[cfg(feature = "temporal")]
#[derive(Clone)]
pub(crate) struct TemporalWorkflowAdapter {
    workflow: WorkflowRunFunction,
    worker: WorkflowWorker,
}

#[cfg(feature = "temporal")]
impl TemporalWorkflowAdapter {
    async fn adapt(
        workflow: WorkflowRunFunction,
        context: WfContext,
        worker: WorkflowWorker,
    ) -> Result<temporal_sdk::WfExitValue<()>, anyhow::Error> {
        let args: Vec<serde_json::Value> = context
            .get_args()
            .iter()
            .map(|payload| serde_json::from_slice(payload.data.as_slice()).unwrap_or_default())
            .collect();

        let dummy_id = Uuid::new_v4().to_string();
        let workflow_context = WorkflowContext::new(
            dummy_id,
            TaskLauncher::Temporal {
                context: Arc::new(context),
                worker,
            },
        );

        // TODO: Rewrite to just use the core worker, since the SDK doesn't do results
        let _result = workflow.0(workflow_context, args).await?;
        Ok(temporal_sdk::WfExitValue::Normal(()))
    }
}

#[cfg(feature = "temporal")]
impl From<TemporalWorkflowAdapter> for temporal_sdk::WorkflowFunction {
    fn from(val: TemporalWorkflowAdapter) -> Self {
        temporal_sdk::WorkflowFunction::new(move |context| {
            TemporalWorkflowAdapter::adapt(val.workflow.clone(), context, val.worker.clone())
        })
    }
}
