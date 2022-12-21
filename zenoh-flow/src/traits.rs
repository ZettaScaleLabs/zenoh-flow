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

use crate::prelude::{Inputs, Outputs};
use crate::runtime::dataflow::instance::io::{Input, Output};
use crate::types::{Configuration, Context, PortId};
use crate::Result;
use async_trait::async_trait;
use futures::Future;
use std::any::Any;
use std::collections::HashMap;
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
    fn try_serialize(&self) -> Result<Vec<u8>>;
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
    fn try_deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// The `Source` trait represents a Source inside Zenoh Flow.
#[async_trait]
pub trait Source: Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        outputs: Outputs,
    ) -> Result<Option<Box<dyn AsyncIteration>>>;
}

/// The `Operator` trait represents an Operator inside Zenoh Flow.
#[async_trait]
pub trait Operator: Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Option<Box<dyn AsyncIteration>>>;
}

/// The `Sink` trait represents a Sink inside Zenoh Flow.
#[async_trait]
pub trait Sink: Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: Inputs,
    ) -> Result<Option<Box<dyn AsyncIteration>>>;
}

/// A `SourceSink` represents Nodes that access the same physical interface to
/// read and write.
#[async_trait]
pub trait SourceSink: Send + Sync {
    async fn setup(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        inputs: HashMap<PortId, Input>,
        outputs: HashMap<PortId, Output>,
    ) -> Result<Option<Box<dyn AsyncIteration>>>;
}

/// Trait wrapping an async closures for node iteration.
///
/// Note: not intended to be directly used by users.
pub trait AsyncIteration: Send + Sync {
    fn call(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns `Result<()>`.
impl<Fut, Fun> AsyncIteration for Fun
where
    Fun: FnMut() -> Fut + Sync + Send,
    Fut: Future<Output = Result<()>> + Send + Sync + 'static,
{
    fn call(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + 'static>> {
        Box::pin((self)())
    }
}
