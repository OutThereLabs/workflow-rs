use crate::state::Extensions;
use crate::state::State;
use anyhow::{anyhow, Error};
use futures_util::future::BoxFuture;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use temporal_sdk::ActExitValue;
use temporal_sdk_core::protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core::protos::temporal::api::common::v1::Payload;

#[derive(Clone, Debug)]
pub struct TaskContext {
    pub(crate) extensions: Arc<Extensions>,
}

impl TaskContext {
    fn extension<T: 'static + Sync + Send>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }

    pub fn state<T: 'static + Sync + Send>(&self) -> Result<State<T>, Error> {
        self.extension::<State<T>>().cloned().ok_or_else(|| {
            tracing::warn!("App state misconfigured");
            anyhow!("App state misconfigured")
        })
    }
}

pub trait TaskHandler<Input, Output>: Send + Sync {
    fn call(
        &self,
        context: TaskContext,
        input: Input,
    ) -> Pin<Box<dyn Future<Output = Result<Output, anyhow::Error>>>>;
}

#[doc(hidden)]
pub struct TaskBuldFunction(pub for<'a> fn(&'a mut TaskBuilder) -> &'a mut TaskBuilder);

type PinBoxJsonFuture = Pin<Box<dyn Future<Output = Result<serde_json::Value, Error>> + Send>>;

#[derive(Clone)]
pub struct TaskRunFunction(pub fn(TaskContext, serde_json::Value) -> PinBoxJsonFuture);

pub trait TaskFactory: Sync + Send {
    fn name(&self) -> &'static str;
    fn queue_name(&self) -> &'static str;
    fn builder(&self, input: serde_json::Value) -> Task;
}

#[derive(Builder, Clone)]
pub struct Task {
    pub task_name: &'static str,
    pub input: serde_json::Value,
    pub handler: TaskRunFunction,
}

impl Task {
    pub(crate) fn call(
        &self,
        context: TaskContext,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, anyhow::Error>> + Send>> {
        Box::pin(self.handler.0(context, self.input.clone()))
    }
}

impl<Input, Output, Func, Fut> TaskHandler<Input, Output> for Func
where
    Func: Fn(TaskContext, Input) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Output, anyhow::Error>> + 'static,
{
    fn call(
        &self,
        context: TaskContext,
        args: Input,
    ) -> Pin<Box<dyn Future<Output = Result<Output, anyhow::Error>>>> {
        Box::pin(self(context, args))
    }
}

#[cfg(feature = "temporal")]
type TemporalBoxFuture = BoxFuture<
    'static,
    Result<
        temporal_sdk::ActExitValue<temporal_sdk_core::protos::temporal::api::common::v1::Payload>,
        anyhow::Error,
    >,
>;

#[cfg(feature = "temporal")]
pub(crate) struct TemporalTaskAdapter<'a>(pub(crate) &'a dyn TaskFactory);

#[cfg(feature = "temporal")]
impl TemporalTaskAdapter<'static> {
    async fn adapt(
        context: temporal_sdk::ActContext,
        payload: temporal_sdk_core::protos::temporal::api::common::v1::Payload,
        task: Task,
    ) -> Result<ActExitValue<Payload>, Error> {
        let mut task = task.clone();
        task.input = serde_json::from_slice(payload.data.as_slice())
            .expect("Could not decode temporal payload as json");

        let extensions: &Arc<Extensions> = context
            .app_data()
            .ok_or_else(|| anyhow!("Need to register extensions as app data"))?;

        let c = TaskContext {
            extensions: extensions.clone(),
        };

        let result = task.call(c).await?;
        Ok(ActExitValue::Normal(result.as_json_payload().unwrap()))
    }
}

#[cfg(feature = "temporal")]
impl temporal_sdk::IntoActivityFunc<serde_json::Value, serde_json::Value, serde_json::Value>
    for TemporalTaskAdapter<'static>
{
    fn into_activity_fn(
        self,
    ) -> Arc<
        dyn Fn(
                temporal_sdk::ActContext,
                temporal_sdk_core::protos::temporal::api::common::v1::Payload,
            ) -> TemporalBoxFuture
            + Send
            + Sync,
    > {
        let task = self.0.builder(serde_json::Value::Null);

        Arc::new(move |context, payload| Box::pin(Self::adapt(context, payload, task.clone())))
    }
}
