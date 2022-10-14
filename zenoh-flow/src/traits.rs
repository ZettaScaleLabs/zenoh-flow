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
/// use zenoh_flow::prelude::{ZFData, Result};
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// impl ZFData for MyString {
///     fn try_serialize(&self) -> Result<Vec<u8>> {
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
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
///
/// impl Deserializable for MyString {
///     fn try_deserialize(bytes: &[u8]) -> Result<MyString>
///     where
///         Self: Sized,
///     {
///         Ok(MyString(
///             String::from_utf8(bytes.to_vec()).map_err(|e| zferror!(ErrorKind::DeseralizationError, e))?,
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

/// TODO(J-Loudet) Documentation.
#[async_trait]
pub trait Node: Send + Sync {
    async fn iteration(&self) -> ZFResult<()>;
}

/// TODO Discuss: Should we make the traits take `&mut self` to allow modifying the Factory? If not,
/// then I don’t think I see a point making the Factory into a trait.
///
/// We could have it be a simple stateless function that generates the nodes.

/// TODO(J-Loudet) Documentation.
#[async_trait]
pub trait SourceFactoryTrait: Send + Sync {
    async fn new_source(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
}

/// TODO(J-Loudet) Documentation.
#[async_trait]
pub trait OperatorFactoryTrait: Send + Sync {
    async fn new_operator(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
}

/// TODO(J-Loudet) Documentation.
#[async_trait]
pub trait SinkFactoryTrait: Send + Sync {
    async fn new_sink(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
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
