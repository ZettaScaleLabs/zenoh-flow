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

use crate::prelude::{Data, Inputs, Message, Outputs};
use crate::types::{Configuration, Context};
use crate::Result as ZFResult;
use async_trait::async_trait;
use futures::Future;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

/// This trait is used to ensure the data can donwcast to [`Any`](`Any`)
/// NOTE: This trait is separate from `ZFData` so that we can provide
/// a `#derive` macro to automatically implement it for the users.
///
/// This can be derived using the `#[derive(ZFData)]`
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
/// not be able to handle the data, serialize and deserialize them when needed.
///
/// Example:
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// use zenoh_flow::prelude::*;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// impl ZFData for MyString {
///     fn try_serialize(&self) -> Result<Vec<u8>> {
///         Ok(self.0.as_bytes().to_vec())
///     }
///
/// fn try_deserialize(bytes: &[u8]) -> Result<MyString>
///     where
///         Self: Sized,
///     {
///         Ok(MyString(
///             String::from_utf8(bytes.to_vec()).map_err(|e| zferror!(ErrorKind::DeserializationError, e))?,
///         ))
///     }
/// }
/// ```
pub trait ZFData: DowncastAny + Debug + Send + Sync {
    /// Tries to serialize the data as `Vec<u8>`
    ///
    /// # Errors
    /// If it fails to serialize an error variant will be returned.
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;

    /// Tries to deserialize from a slice of `u8`.
    ///
    /// # Errors
    /// If it fails to deserialize an error variant will be returned.
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

/// The `Source` trait represents a Source of data in Zenoh Flow.
/// Sources only possess `Outputs` and their purpose is to fetch data from the external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
/// A struct implementing the Source trait typically needs to keep a reference to the
/// `Output` it needs.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// use std::convert::TryInto;
/// static SOURCE: &str = "Counter";
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct ZFUsize(pub usize);
///
/// impl ZFData for ZFUsize {
/// fn try_serialize(&self) -> Result<Vec<u8>> {
///     Ok(self.0.to_ne_bytes().to_vec())
/// }
///
/// fn try_deserialize(bytes: &[u8]) -> Result<Self>
/// where
///     Self: Sized,
/// {
///     let value = usize::from_ne_bytes(
///         bytes
///             .try_into()
///             .map_err(|e| zferror!(ErrorKind::DeseralizationError, "{}", e))?,
///     );
///     Ok(ZFUsize(value))
/// }
/// }
///
/// pub struct MySource {
///   output: Output,    // A Source would have one or more outputs.
///   // The state could go in such structure.
///   // state: Arc<Mutex<T>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Source for MySource {
///
///   fn new(
///        _context: &mut Context,
///        _configuration: &Option<Configuration>,
///        mut outputs: Outputs,
///     ) -> Result<Option<Self>> {
///         let output = outputs.take(SOURCE).unwrap();
///         Ok(Some(Self{output}))
///     }
///
///   async fn iteration(&self) -> Result<()> {
///     // To mutate the state, first lock it.
///     // let state = self.state.lock().await;
///     // from the state the Source may read information from the external
///     // world, i.e., interacting with I/O devices.
///     async_std::task::sleep(std::time::Duration::from_secs(1)).await;
///
///     self.output.send_async(ZFUsize(1), None).await?;
///     Ok(())
///   }
/// }
/// ```
#[async_trait]
pub trait Source: Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Outputs`, produce a new *Source*.
    ///
    /// Sources only possess `Outputs` and their purpose is to fetch data from the external world.
    ///
    /// Sources are **started last** when initiating a data flow. This is to prevent data loss: if a
    /// Source is started before its downstream nodes then the data it would send before said downstream
    /// nodes are up would be lost.
    fn new(
        context: &mut Context,
        configuration: &Option<Configuration>,
        outputs: Outputs,
    ) -> ZFResult<Option<Self>>
    where
        Self: Sized;

    /// The `iteration` that is repeatedly called by Zenoh-Flow.
    /// Here the source can interact with the extrnal world and send
    /// data over its outputs.
    async fn iteration(&self) -> ZFResult<()>;
}

/// A `CastSource` is an internal structure used to foster code reuse
/// in the runtime. I.e., it allows all `Source`s to be `Node`s.
pub(crate) struct CastSource(Arc<dyn Source>);

impl From<Arc<dyn Source>> for CastSource {
    fn from(other: Arc<dyn Source>) -> Self {
        Self(other)
    }
}

#[async_trait]
impl Node for CastSource {
    async fn iteration(&self) -> ZFResult<()> {
        self.0.iteration().await
    }
}

/// The `Sink` trait represents a Sink of data in Zenoh Flow.
/// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
/// external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
/// A struct implementing the Sink trait typically needs to keep a reference to the
/// `Output` it needs.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// static SOURCE: &str = "Counter";
///
/// struct GenericSink {
///     input: Input,
/// }
///
/// #[async_trait]
/// impl Sink for GenericSink {
///
///     fn new(
///         _context: &mut Context,
///         _configuration: &Option<Configuration>,
///         mut inputs: Inputs,
///     ) -> Result<Option<Self>> {
///         let input = inputs.take(SOURCE).unwrap();
///
///         Ok(Some(GenericSink {
///             input,
///         }))
///     }
///
///     async fn iteration(&self) -> Result<()> {
///
///         if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
///             println!("Data {:?}", msg);
///         }
///
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink: Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Inputs`, produce a new **Sink**.
    ///
    /// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
    /// external world.
    ///
    /// Sinks are **started first** when initiating a data flow. As they are at the end of the chain of
    /// computations, by starting them first we ensure that no data is lost.
    fn new(
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
    ) -> ZFResult<Option<Self>>
    where
        Self: Sized;

    /// The `iteration` that is repeatedly called by Zenoh-Flow.
    /// Here the source can interact with the extrnal world and retrieve
    /// data from its inputs.
    async fn iteration(&self) -> ZFResult<()>;
}

/// A `CastSink` is an internal structure used to foster code reuse
/// in the runtime. I.e., it allows all `Sink`s to be `Node`s.
pub(crate) struct CastSink(Arc<dyn Sink>);

impl From<Arc<dyn Sink>> for CastSink {
    fn from(other: Arc<dyn Sink>) -> Self {
        Self(other)
    }
}

#[async_trait]
impl Node for CastSink {
    async fn iteration(&self) -> ZFResult<()> {
        self.0.iteration().await
    }
}

/// The `Operator` trait represents an Operator inside Zenoh Flow.
/// Operators are at the heart of a data flow, they carry out computations on the data they receive
/// before sending them out to the next downstream node.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Operator trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///  # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// static SOURCE: &str = "Counter";
/// static DESTINATION: &str = "Counter";
///
/// struct NoOp {
///     input: Input,
///     output: Output,
/// }
///
/// #[async_trait]
/// impl Operator for NoOp {
///     fn new(
///         _context: &mut Context,
///         _configuration: &Option<Configuration>,
///         mut inputs: Inputs,
///         mut outputs: Outputs,
///     ) -> Result<Option<Self>> {
///         Ok(Some(NoOp {
///             input: inputs.take(SOURCE).unwrap(),
///             output: outputs.take(DESTINATION).unwrap(),
///         }))
///     }
///
///     async fn iteration(&self) -> Result<()> {
///         if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
///             self.output.send_async((*msg).clone(), None).await?;
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Operator: Send + Sync {
    /// For a `Context`, a `Configuration`, a set of `Inputs` and `Outputs`, produce a new **Operator**.
    ///
    /// Operators are at the heart of a data flow, they carry out computations on the data they receive
    /// before sending them out to the next downstream node.
    ///
    /// The Operators are started *before the Sources* such that they are active before the first data
    /// are produced.
    fn new(
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> ZFResult<Option<Self>>
    where
        Self: Sized;

    /// The `iteration` that is repeatedly called by Zenoh-Flow.
    /// Here the operator can interact retrieve data from its inputs,
    /// compute over them, and send the results on its outputs.
    async fn iteration(&self) -> ZFResult<()>;
}

/// A `CastOperator` is an internal structure used to foster code reuse
/// in the runtime. I.e., it allows all `Operator`s to be `Node`s.
pub(crate) struct CastOperator(Arc<dyn Operator>);

impl From<Arc<dyn Operator>> for CastOperator {
    fn from(other: Arc<dyn Operator>) -> Self {
        Self(other)
    }
}

#[async_trait]
impl Node for CastOperator {
    async fn iteration(&self) -> ZFResult<()> {
        self.0.iteration().await
    }
}

/// A `Node` is defined by its `iteration` that is repeatedly called by Zenoh-Flow.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Node trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// * Note: * not intended to be directly used by users.
/// ```
#[async_trait]
pub trait Node: Send + Sync {
    async fn iteration(&self) -> ZFResult<()>;
}

#[async_trait]
impl Node for dyn Source {
    async fn iteration(&self) -> ZFResult<()> {
        Source::iteration(self).await
    }
}

#[async_trait]
impl Node for dyn Sink {
    async fn iteration(&self) -> ZFResult<()> {
        Sink::iteration(self).await
    }
}

#[async_trait]
impl Node for dyn Operator {
    async fn iteration(&self) -> ZFResult<()> {
        Operator::iteration(self).await
    }
}

/// Trait wrapping an async closures for sender callback, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
type AsyncCallbackOutput = ZFResult<(Data, Option<u64>)>;

pub trait AsyncCallbackTx: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = AsyncCallbackOutput> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns
/// `ZFResult<()>`.
/// This "converts" any `async move { ... }` to `AsyncCallbackTx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncCallbackTx for Fun
where
    Fun: Fn() -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<(Data, Option<u64>)>> + Send + Sync + 'static,
{
    fn call(
        &self,
    ) -> Pin<Box<dyn Future<Output = ZFResult<(Data, Option<u64>)>> + Send + Sync + 'static>> {
        Box::pin((self)())
    }
}

/// Trait describing the functions we are expecting to call upon receiving a message.
///
/// NOTE: Users are encouraged to provide a closure instead of implementing this trait.
pub trait InputCallback: Send + Sync {
    fn call(
        &self,
        arg: Message,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>>;
}

/// Implementation of InputCallback for any async closure that takes `Message` as parameter and
/// returns `ZFResult<()>`. This "converts" any `async move |msg| { ... Ok() }` into an
/// `InputCallback`.
///
/// This allows users to provide a closure instead of implementing the trait.
impl<Fut, Fun> InputCallback for Fun
where
    Fun: Fn(Message) -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<()>> + Send + Sync + 'static,
{
    fn call(
        &self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>> {
        Box::pin((self)(message))
    }
}

/// TODO Documentation: Output callback expects nothing (except for a trigger) and returns some
/// Data. As it’s a callback, Zenoh-Flow will take care of sending it.
pub trait OutputCallback: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>>;
}

/// TODO Documentation: implementation of OutputCallback for closures, it makes it easier to write
/// these functions.
impl<Fut, Fun> OutputCallback for Fun
where
    Fun: Fn() -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<Data>> + Send + Sync + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>> {
        Box::pin((self)())
    }
}
