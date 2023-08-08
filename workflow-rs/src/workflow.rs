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
use temporal_sdk::{ActivityOptions, LocalActivityOptions, WfContext, Worker};
use temporal_sdk_core::protos::coresdk::activity_result::activity_resolution;
use temporal_sdk_core::protos::coresdk::{activity_result, AsJsonPayloadExt};
use temporal_sdk_core_api::telemetry::{Logger, OtelCollectorOptions, TraceExporter};

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
        match self {
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

                let telemetry_options = TelemetryOptionsBuilder::default()
                    .tracing(TraceExportConfig {
                        filter: "info".to_owned(),
                        exporter: TraceExporter::Otel(OtelCollectorOptions {
                            url: "grpc://localhost:4317".parse().unwrap(),
                            headers: Default::default(),
                            metric_periodicity: None,
                        }),
                    })
                    .logging(Logger::Console {
                        filter: "info".to_owned(),
                    })
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
    Temporal(Arc<temporal_sdk::WfContext>),
}

#[derive(Clone)]
pub struct WorkflowContext {
    task_launcher: TaskLauncher,
}

impl<'a> WorkflowContext {
    pub fn new(task_launcher: TaskLauncher) -> Self {
        Self { task_launcher }
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

    pub async fn local_task<F: TaskFactory, I: serde::Serialize>(
        &self,
        factory: F,
        input: I,
    ) -> Result<serde_json::Value, Error> {
        self.local_task_by_name(factory.name(), serde_json::json!(input))
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
            TaskLauncher::Temporal(context) => {
                Self::temporal_task(name, queue, input, context).await
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
            TaskLauncher::Temporal(context) => {
                Self::temporal_local_task(name, input, context).await
            }
        }
    }

    pub async fn sleep(&self, duration: Duration) {
        match &self.task_launcher {
            TaskLauncher::InMemory(_) => {
                tokio::time::sleep(duration).await;
            }
            #[cfg(feature = "temporal")]
            TaskLauncher::Temporal(context) => {
                let _ = context.timer(duration).await;
            }
        }
    }

    // pub async fn workflow<F: WorkflowFactory>(
    //     &self,
    //     id: String,
    //     factory: F,
    //     input: Vec<serde_json::Value>,
    // ) -> Result<(), Error> {
    //     self.workflow_by_name(id, factory.name(), input).await
    // }
    //
    // pub async fn workflow_by_name(
    //     &self,
    //     id: String,
    //     name: &'static str,
    //     input: Vec<serde_json::Value>,
    // ) -> Result<(), Error> {
    //     match &self.task_launcher {
    //         TaskLauncher::InMemory(registry) => {
    //             let registry = registry.clone();
    //             self.clone().in_memory_workflow(name, input, registry).await
    //         }
    //         #[cfg(feature = "temporal")]
    //         TaskLauncher::Temporal(context) => {
    //             Self::temporal_workflow(id, name, input, &context).await
    //         }
    //     }
    // }

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

    // async fn in_memory_workflow(
    //     self,
    //     name: &'static str,
    //     input: Vec<Value>,
    //     registry: Arc<WorkflowRegistry>,
    // ) -> Result<(), Error> {
    //     match registry.workflow_factories.get(name) {
    //         Some(workflow) => {
    //             let workflow = workflow.builder(vec![]);
    //             let _ = workflow.handler.0(self, input).await?;
    //             Ok(())
    //         }
    //         None => Err(anyhow!("No task registed for {name}")),
    //     }
    // }

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

    // #[cfg(feature = "temporal")]
    // async fn temporal_workflow(
    //     id: String,
    //     name: &'static str,
    //     input: Vec<Value>,
    //     context: &'a WfContext,
    // ) -> Result<(), Error> {
    //     let workflow_options = ChildWorkflowOptions {
    //         workflow_type: name.to_owned(),
    //         workflow_id: id,
    //         input: input
    //             .iter()
    //             .map(|value| value.as_json_payload().unwrap())
    //             .collect(),
    //         ..Default::default()
    //     };
    //     let result = context
    //         .child_workflow(workflow_options)
    //         .start(context)
    //         .await;
    //
    //     match result.status {
    //         ChildWorkflowStartStatus::Succeeded(_) => Ok(()),
    //         other => Err(anyhow!("Task failed: {other:#?}")),
    //     }
    // }
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
}

#[cfg(feature = "temporal")]
impl TemporalWorkflowAdapter {
    async fn adapt(
        workflow: WorkflowRunFunction,
        context: WfContext,
    ) -> Result<temporal_sdk::WfExitValue<()>, anyhow::Error> {
        let args: Vec<serde_json::Value> = context
            .get_args()
            .iter()
            .map(|payload| serde_json::from_slice(payload.data.as_slice()).unwrap_or_default())
            .collect();

        let workflow_context = WorkflowContext::new(TaskLauncher::Temporal(Arc::new(context)));

        // TODO: Rewrite to just use the core worker, since the SDK doesn't do results
        let _result = workflow.0(workflow_context, args).await?;
        Ok(temporal_sdk::WfExitValue::Normal(()))
    }
}

#[cfg(feature = "temporal")]
impl From<TemporalWorkflowAdapter> for temporal_sdk::WorkflowFunction {
    fn from(val: TemporalWorkflowAdapter) -> Self {
        temporal_sdk::WorkflowFunction::new(move |context| {
            TemporalWorkflowAdapter::adapt(val.workflow.clone(), context)
        })
    }
}
