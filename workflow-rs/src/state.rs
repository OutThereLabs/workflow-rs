use crate::data::Data;
use crate::task::TaskContext;
use anyhow::Error;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::ops;
use std::sync::Arc;

pub struct State<T>(Arc<T>);

impl<T> State<T> {
    /// Create new `State` instance.
    pub fn new(state: T) -> State<T> {
        State(Arc::new(state))
    }

    /// Get reference to inner app data.
    pub fn get_ref(&self) -> &T {
        self.0.as_ref()
    }

    /// Convert to the internal Arc<T>
    pub fn into_inner(self) -> Arc<T> {
        self.0
    }
}

impl<T> ops::Deref for State<T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Arc<T> {
        &self.0
    }
}

impl<T> Clone for State<T> {
    fn clone(&self) -> State<T> {
        State(self.0.clone())
    }
}

impl<T> From<Arc<T>> for State<T> {
    fn from(arc: Arc<T>) -> Self {
        State(arc)
    }
}

impl<T> fmt::Debug for State<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "State: {:?}", self.0)
    }
}

impl<T> fmt::Display for State<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

pub trait FromTask: Sized {
    fn from_task(
        _context: &TaskContext,
        input: serde_json::Value,
    ) -> std::result::Result<Self, Error>;
}

impl FromTask for TaskContext {
    fn from_task(
        context: &TaskContext,
        _input: serde_json::Value,
    ) -> std::result::Result<Self, Error> {
        Ok(context.clone())
    }
}

impl<T: serde::de::DeserializeOwned> FromTask for Data<T> {
    fn from_task(
        _context: &TaskContext,
        input: serde_json::Value,
    ) -> std::result::Result<Self, Error> {
        Ok(Data(serde_json::from_value(input)?))
    }
}

impl<T: 'static + Send + Sync> FromTask for State<T> {
    fn from_task(
        context: &TaskContext,
        _input: serde_json::Value,
    ) -> std::result::Result<Self, Error> {
        let state = context.state()?;
        Ok(state)
    }
}

#[derive(Default)]
pub struct Extensions(HashMap<TypeId, Box<dyn Any + Sync + Send>>);

impl Extensions {
    pub(crate) fn insert<T: 'static + Sync + Send>(&mut self, data: T) {
        self.0.insert(data.type_id(), Box::new(data));
    }

    pub(crate) fn get<T: 'static + Sync + Send>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|boxed| (&**boxed as &(dyn Any + 'static)).downcast_ref())
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Extensions {{ }}")
    }
}
