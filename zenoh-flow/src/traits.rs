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
use crate::types::{Configuration, Context};
use crate::Result;
use async_trait::async_trait;
use std::any::Any;
use std::fmt::Debug;

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
///             String::from_utf8(bytes.to_vec()).map_err(|e| zferror!(ErrorKind::DeseralizationError, e))?,
///         ))
///     }
/// }
/// ```
pub trait ZFData: DowncastAny + Debug + Send + Sync {
    /// Tries to serialize the data as `Vec<u8>`
    ///
    /// # Errors
    /// If it fails to serialize an error variant will be returned.
    fn try_serialize(&self) -> Result<Vec<u8>>;

    /// Tries to deserialize from a slice of `u8`.
    ///
    /// # Errors
    /// If it fails to deserialize an error variant will be returned.
    fn try_deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// The `Source` trait represents a Source of data in Zenoh Flow. Sources only possess `Outputs` and
/// their purpose is to fetch data from the external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Source trait typically needs to keep a reference to the `Output` it
/// needs.
///
/// # Example
///
/// ```no_run
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// use std::convert::TryInto;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct ZFUsize(pub usize);
///
/// impl ZFData for ZFUsize {
///     fn try_serialize(&self) -> Result<Vec<u8>> {
///         Ok(self.0.to_ne_bytes().to_vec())
///     }
///
///     fn try_deserialize(bytes: &[u8]) -> Result<Self>
///     where
///         Self: Sized,
///     {
///         let value = usize::from_ne_bytes(
///             bytes
///                 .try_into()
///                 .map_err(|e| zferror!(ErrorKind::DeseralizationError, "{}", e))?,
///         );
///         Ok(ZFUsize(value))
///     }
/// }
///
/// pub struct MySource {
///     output: Output,    // A Source would have one or more outputs.
///     // The state could go in such structure.
///     // state: Arc<Mutex<T>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Source for MySource {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         let output = outputs.take("out").expect("No output called 'out' found");
///         Ok(Self { output })
///     }
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MySource {
///     async fn iteration(&self) -> Result<()> {
///         // To mutate the state, first lock it.
///         //
///         // let state = self.state.lock().await;
///         //
///         // The state is a way for the Source to read information from the external world, i.e.,
///         // interacting with I/O devices. We mimick an asynchronous iteraction with a sleep.
///         async_std::task::sleep(std::time::Duration::from_secs(1)).await;
///
///         self.output.send_async(ZFUsize(1), None).await?;
///         Ok(())
///     }
/// }
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// export_source!(MySource);
/// ```
#[async_trait]
pub trait Source: Node + Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Outputs`, produce a new *Source*.
    ///
    /// Sources only possess `Outputs` and their purpose is to fetch data from the external world.
    ///
    /// Sources are **started last** when initiating a data flow. This is to prevent data loss: if a
    /// Source is started before its downstream nodes then the data it would send before said
    /// downstream nodes are up would be lost.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        outputs: Outputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// The `Sink` trait represents a Sink of data in Zenoh Flow.
///
/// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
/// external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Sink trait typically needs to keep a reference to the `Input` it
/// needs.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// struct GenericSink {
///     input: Input,
/// }
///
/// #[async_trait]
/// impl Sink for GenericSink {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut inputs: Inputs,
///     ) -> Result<Self> {
///         let input = inputs.take("in").expect("No input called 'in' found");
///
///         Ok(GenericSink { input })
///     }
/// }
///
/// #[async_trait]
/// impl Node for GenericSink {
///     async fn iteration(&self) -> Result<()> {
///         if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
///             println!("Data {:?}", msg);
///         }
///
///         Ok(())
///     }
/// }
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// export_sink!(GenericSink);
/// ```
#[async_trait]
pub trait Sink: Node + Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Inputs`, produce a new **Sink**.
    ///
    /// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
    /// external world.
    ///
    /// Sinks are **started first** when initiating a data flow. As they are at the end of the chain of
    /// computations, by starting them first we ensure that no data is lost.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        inputs: Inputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// The `Operator` trait represents an Operator inside Zenoh-Flow.
///
/// Operators are at the heart of a data flow, they carry out computations on the data they receive
/// before sending them out to the next downstream node.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Operator trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// struct NoOp {
///     input: Input,
///     output: Output,
/// }
///
/// #[async_trait]
/// impl Operator for NoOp {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut inputs: Inputs,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         Ok(NoOp {
///             input: inputs.take("in").expect("No input called 'in' found"),
///             output: outputs.take("out").expect("No output called 'out' found"),
///         })
///     }
/// }
/// #[async_trait]
/// impl Node for NoOp {
///     async fn iteration(&self) -> Result<()> {
///         if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
///             self.output.send_async((*msg).clone(), None).await?;
///         }
///         Ok(())
///     }
/// }
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// export_operator!(NoOp);
/// ```
#[async_trait]
pub trait Operator: Node + Send + Sync {
    /// For a `Context`, a `Configuration`, a set of `Inputs` and `Outputs`, produce a new
    /// **Operator**.
    ///
    /// Operators are at the heart of a data flow, they carry out computations on the data they
    /// receive before sending them out to the next downstream node.
    ///
    /// The Operators are started *before the Sources* such that they are active before the first
    /// data are produced.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// A `Node` is defined by its `iteration` that is repeatedly called by Zenoh-Flow.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Node trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// For usage examples see [`Operator`](`Operator`), [`Source`](`Source`) or [`Sink`](`Sink`) traits.
/// ```
#[async_trait]
pub trait Node: Send + Sync {
    async fn iteration(&self) -> Result<()>;
}
