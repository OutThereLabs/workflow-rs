use crate::in_memory::InMemoryExecutorStorage;
use crate::state::Extensions;
use crate::state::State;
use crate::task::TaskFactory;
use crate::workflow::{TaskLauncher, WorkflowWorker};
use crate::workflow::{WorkflowContext, WorkflowFactory};
use anyhow::{anyhow, Error};
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use temporal_client::{Client, RetryClient, WorkflowClientTrait};
use temporal_sdk_core::protos::coresdk::AsJsonPayloadExt;

#[derive(Clone)]
pub struct WorkflowRegistry {
    pub(crate) executor: WorkflowExecutor,
    pub(crate) workflow_factories: HashMap<&'static str, &'static dyn WorkflowFactory>,
    pub(crate) task_factories: HashMap<&'static str, &'static dyn TaskFactory>,
    pub(crate) extensions: Arc<Extensions>,
    build_id: &'static str,
}

#[derive(Default)]
pub struct WorkflowRegistryBuilder {
    extensions: Extensions,
    workflow_factories: HashMap<&'static str, &'static dyn WorkflowFactory>,
    task_factories: HashMap<&'static str, &'static dyn TaskFactory>,
    build_id: &'static str,
}

impl WorkflowRegistryBuilder {
    fn build(self, executor: WorkflowExecutor) -> WorkflowRegistry {
        WorkflowRegistry {
            executor,
            workflow_factories: self.workflow_factories,
            task_factories: self.task_factories,
            extensions: Arc::new(self.extensions),
            build_id: self.build_id,
        }
    }

    pub fn state<T: 'static + Sync + Send>(&mut self, data: T) {
        self.extensions.insert(State::new(data))
    }

    pub fn build_id(&mut self, build_id: &'static str) {
        self.build_id = build_id;
    }
}

impl Debug for WorkflowRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("Workflow registry: [{:?}]", self.extensions).as_str())
    }
}

impl WorkflowRegistry {
    #[cfg(feature = "temporal")]
    pub async fn from_env<F: FnOnce(&mut WorkflowRegistryBuilder)>(
        config: F,
    ) -> Result<Arc<WorkflowRegistry>, Error> {
        use temporal_sdk_core::{ClientOptionsBuilder, Url};

        if let Ok(url) = std::env::var("TEMPORAL_HOST") {
            let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or("default".to_owned());
            let client_name = std::env::var("SERVICE_INSTANCE_ID")?;
            let client_version = std::env::var("SERVICE_INSTANCE_VERSION").unwrap_or_default();

            let client = ClientOptionsBuilder::default()
                .target_url(Url::parse(&url)?)
                .client_name(client_name)
                .client_version(client_version)
                .build()
                .unwrap()
                .connect(namespace, None, None)
                .await?;
            Ok(Self::new_temporal(client, config))
        } else {
            Ok(Self::new_in_memory(config))
        }
    }

    #[cfg(not(feature = "temporal"))]
    pub async fn from_env<F: FnOnce(&mut WorkflowRegistryBuilder) -> ()>(
        config: F,
    ) -> Result<Arc<WorkflowRegistry>, Error> {
        Ok(Self::new_in_memory(config))
    }

    pub fn new_in_memory<F: FnOnce(&mut WorkflowRegistryBuilder)>(
        config: F,
    ) -> Arc<WorkflowRegistry> {
        let mut builder = WorkflowRegistryBuilder::default();
        config(&mut builder);

        Arc::new(builder.build(WorkflowExecutor::InMemory(
            InMemoryExecutorStorage::default(),
        )))
    }

    #[cfg(feature = "temporal")]
    pub fn new_temporal<F: FnOnce(&mut WorkflowRegistryBuilder)>(
        client: RetryClient<Client>,
        config: F,
    ) -> Arc<WorkflowRegistry> {
        let mut builder = WorkflowRegistryBuilder::default();

        config(&mut builder);

        Arc::new(builder.build(WorkflowExecutor::Temporal { client }))
    }

    pub async fn run_until_done<I: ToString>(
        &self,
        workflow_id: I,
        queue: &'static str,
        timeout: Duration,
    ) -> Result<(), Error> {
        match &self.executor {
            WorkflowExecutor::InMemory(storage) => storage.execute_all().await,
            #[cfg(feature = "temporal")]
            WorkflowExecutor::Temporal { client, .. } => {
                use tokio::time::sleep;

                let check_future = async {
                    let start_time = std::time::SystemTime::now();
                    while std::time::SystemTime::now()
                        .duration_since(start_time)
                        .unwrap_or_default()
                        < timeout
                    {
                        let history = client
                            .describe_workflow_execution(workflow_id.to_string(), None)
                            .await?;

                        match history
                            .workflow_execution_info
                            .clone()
                            .unwrap_or_default()
                            .status
                        {
                            // WorkflowExecutionStatus::Completed,
                            2 => {
                                tracing::debug!(
                                    "WOrkflow {} has status of completed",
                                    workflow_id.to_string()
                                );
                                return Ok(());
                            }
                            // WorkflowExecutionStatus::Running,
                            1 => {
                                sleep(Duration::from_millis(100)).await;
                            }
                            _other => {
                                let info = history.workflow_execution_info.unwrap_or_default();
                                return Err(anyhow!("Workflow execution failed: {info:?}"));
                            }
                        }
                    }
                    Err(anyhow!("Timed out"))
                };

                let worker = self
                    .worker(queue, self.build_id)
                    .map_err(|error| anyhow!("Worker error: {}", error))?;

                let worker_future =
                    worker.run(self.workflow_factories.clone(), self.task_factories.clone());

                tokio::select! {
                    _ = worker_future => {
                         tracing::info!("Worker finished");
                    }
                    result = check_future => {
                        match result {
                            Ok(_) => {
                                tracing::info!("Worker finished");
                            },
                            Err(error) => {
                                tracing::warn!("Error checking for output: {}", error);
                            }
                        }
                    },
                };

                Ok(())
            }
        }
    }
}

impl WorkflowRegistryBuilder {
    pub fn register_task<T: TaskFactory + 'static>(&mut self, task_factory: &'static T) {
        let task_name = task_factory.name();

        self.task_factories.insert(task_name, task_factory);
    }

    pub fn register_workflow<T: WorkflowFactory + 'static>(
        &mut self,
        workflow_builder: &'static T,
    ) {
        self.workflow_factories
            .insert(workflow_builder.name(), workflow_builder);
    }
}

impl WorkflowRegistry {
    pub async fn await_workflow<T: WorkflowFactory + 'static, I: ToString, A: Serialize>(
        self: &Arc<WorkflowRegistry>,
        id: I,
        workflow_builder: &'static T,
        args: Vec<A>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        self.start_workflow(id.to_string(), workflow_builder, args)
            .await?;
        self.run_until_done(id.to_string(), workflow_builder.queue_name(), timeout)
            .await?;
        Ok(())
    }

    pub async fn start_workflow<T: WorkflowFactory + 'static, I: ToString, A: Serialize>(
        self: &Arc<WorkflowRegistry>,
        id: I,
        workflow_builder: &'static T,
        args: Vec<A>,
    ) -> Result<(), anyhow::Error> {
        let args = args.into_iter().map(|arg| json!(arg)).collect();
        self.executor
            .clone()
            .start_workflow(
                id.to_string(),
                workflow_builder.name(),
                workflow_builder.queue_name(),
                args,
                self.clone(),
            )
            .await?;
        Ok(())
    }

    pub async fn start_named_workflow<T: WorkflowFactory + 'static, I: ToString, A: Serialize>(
        self: &Arc<WorkflowRegistry>,
        id: I,
        name: &'static str,
        queue: &'static str,
        args: Vec<A>,
    ) -> Result<(), anyhow::Error> {
        let args = args.into_iter().map(|arg| json!(arg)).collect();
        self.executor
            .clone()
            .start_workflow(
                id.to_string(),
                name,
                queue,
                args,
                self.clone(),
            )
            .await?;
        Ok(())
    }

    pub fn worker(
        &self,
        queue: &'static str,
        build_id: &'static str,
    ) -> Result<WorkflowWorker, anyhow::Error> {
        match &self.executor {
            WorkflowExecutor::InMemory(storage) => Ok(WorkflowWorker::InMemory(storage.clone())),
            #[cfg(feature = "temporal")]
            WorkflowExecutor::Temporal { client } => Ok(WorkflowWorker::Temporal {
                extensions: self.extensions.clone(),
                client: client.clone(),
                queue,
                build_id,
            }),
        }
    }

    pub async fn run_worker(&self, queue: &'static str) -> Result<(), Error> {
        self.worker(queue, self.build_id)?
            .run(self.workflow_factories.clone(), self.task_factories.clone())
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum WorkflowExecutor {
    InMemory(InMemoryExecutorStorage),
    #[cfg(feature = "temporal")]
    Temporal {
        client: temporal_client::RetryClient<temporal_client::Client>,
    },
}

impl WorkflowExecutor {
    #[tracing::instrument(skip(self, registry))]
    pub(crate) async fn start_workflow(
        self,
        id: String,
        name: &str,
        queue: &str,
        args: Vec<serde_json::Value>,
        registry: Arc<WorkflowRegistry>,
    ) -> Result<(), Error> {
        match &self {
            Self::InMemory(storage) => {
                let workflow_factory = registry
                    .workflow_factories
                    .get(name)
                    .ok_or_else(|| anyhow!("Workflow {name} not found"))?;
                let context = WorkflowContext::new(id, TaskLauncher::InMemory(registry.clone()));
                let workflow = workflow_factory.builder(args);
                let handle = tokio::spawn(async move { workflow.execute(context).await });
                storage.add_task(handle);

                Ok(())
            }
            #[cfg(feature = "temporal")]
            Self::Temporal { client } => {
                client
                    .start_workflow(
                        args.iter()
                            .map(|value| value.as_json_payload().unwrap())
                            .collect(),
                        queue.to_owned(),
                        id,
                        name.to_owned(),
                        None,
                        Default::default(),
                    )
                    .await?;
                Ok(())
            }
        }
    }
}
