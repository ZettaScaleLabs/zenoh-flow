//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use crate::runtime::Context;
use crate::{Configuration, Inputs, Outputs, ZFResult};
use async_std::sync::Arc;
use async_trait::async_trait;
use futures::Future;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;

/// This trait is used to ensure the data can donwcast to [`Any`](`Any`)
/// NOTE: This trait is separate from `ZFDataTrait` so that we can provide
/// a `#derive` macro to automatically implement it for the users.
///
/// This can be derived using the `#derive(ZFData)`
///
/// Example::
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// ```
pub trait DowncastAny {
    /// Donwcast as a reference to [`Any`](`Any`)
    fn as_any(&self) -> &dyn Any;

    /// Donwcast as a mutable reference to [`Any`](`Any`)
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// This trait abstracts the user's data type inside Zenoh Flow.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the data, and serialize them when needed.
///
/// Example:
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// use zenoh_flow::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// impl ZFData for MyString {
///     fn try_serialize(&self) -> zenoh_flow::ZFResult<Vec<u8>> {
///         Ok(self.0.as_bytes().to_vec())
///     }
/// }
/// ```
pub trait ZFData: DowncastAny + Debug + Send + Sync {
    /// Tries to serialize the data as `Vec<u8>`
    ///
    /// # Errors
    /// If it fails to serialize an error variant will be returned.
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;
}

/// This trait abstract user's type deserialization.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the data, and deserialize them when needed.
///
/// Example:
/// ```no_run
///
/// use zenoh_flow::{Deserializable, ZFResult, ZFError};
/// use zenoh_flow::zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
///
/// impl Deserializable for MyString {
///     fn try_deserialize(bytes: &[u8]) -> ZFResult<MyString>
///     where
///         Self: Sized,
///     {
///         Ok(MyString(
///             String::from_utf8(bytes.to_vec()).map_err(|_| ZFError::DeseralizationError)?,
///         ))
///     }
/// }
/// ```
pub trait Deserializable {
    /// Tries to deserialize from a slice of `u8`.
    ///
    /// # Errors
    /// If it fails to deserialize an error variant will be returned.
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

/// This trait abstracts the user's state type inside Zenoh Flow.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the state.
///
/// It can be easily derived.
///
/// Example:
///
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFState;
/// #[derive(Debug, Clone, ZFState)]
/// pub struct MyState;
/// ```
///
pub trait ZFState: Debug + Send + Sync {
    /// Donwcast as reference to [`Any`](`Any`)
    fn as_any(&self) -> &dyn Any;

    /// Donwcast as mutable reference to [`Any`](`Any`)
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// The `Node` trait represents a generic node in the data flow graph.
/// It contains functions that are common between Operator, Sink and Source.
/// It has to be implemented for each node within a graph.
#[async_trait]
pub trait Node {
    // /// This method is used to initialize the state of the node.
    // /// It is called by the Zenoh Flow runtime when initializing the data flow
    // /// graph.
    // /// An example of node state is files that should be opened, connection
    // /// to devices or internal configuration.
    // ///
    // /// # Errors
    // /// If it fails to initialize an error variant will be returned.
    // fn initialize(&self, configuration: &Option<Configuration>) -> ZFResult<State>;

    /// This method is used to finalize a node. It is called by the Zenoh Flow
    /// runtime when tearing down the data flow graph.
    ///
    /// An example of node state to finalize is files to be closed, clean-up of
    /// libraries, or devices.
    ///
    /// # Errors
    /// If it fails to finalize an error variant will be returned.
    async fn finalize(&self) -> ZFResult<()>;
}

/// The `Source` trait represents a Source inside Zenoh Flow.
#[async_trait]
pub trait Source: Node + Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        outputs: Outputs,
    ) -> Arc<dyn AsyncIteration>;
}

/// The `Operator` trait represents an Operator inside Zenoh Flow.
#[async_trait]
pub trait Operator: Node + Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Arc<dyn AsyncIteration>;
}

/// The `Sink` trait represents a Sink inside Zenoh Flow.
#[async_trait]
pub trait Sink: Node + Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
    ) -> Arc<dyn AsyncIteration>;
}

/// A `SourceSink` represents Nodes that access the same physical interface to
/// read and write.
#[async_trait]
pub trait SourceSink: Node + Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Arc<dyn AsyncIteration>;
}

/// Trait wrapping an async closures for node iteration, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
pub trait AsyncIteration: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns
/// `ZFResult<()>`.
/// This "converts" any `async move { ... }` to `AsyncCallbackTx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncIteration for Fun
where
    Fun: FnOnce() -> Fut + Sync + Send + Clone,
    Fut: Future<Output = ZFResult<()>> + Send + Sync + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>> {
        Box::pin(self.clone()())
    }
}
