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

use crate::prelude::{ErrorKind, Message, PortId, ZFData};
use crate::types::{Data, DataMessage, LinkMessage};
use crate::{zferror, Result};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use uhlc::Timestamp;

/// The [`Inputs`](`Inputs`) structure contains all the receiving channels we created for a
/// [`Sink`](`Sink`) or an [`Operator`](`Operator`).
///
/// To access these underlying channels, two methods are available:
/// - `take`: this will return an `Input<T>` where `T` implements [`ZFData`](`ZFData`),
/// - `take_raw`: this will return an [`InputRaw`](`InputRaw`) — a type agnostic receiver.
///
/// Choosing between `take` and `take_raw` is a trade-off between convenience and performance: an
/// `Input<T>` conveniently receives instances of `T` and is thus less performant as Zenoh-Flow has
/// to manipulate the data; an `InputRaw` is more performant but all the manipulation must be
/// performed in the code of the Node. An `InputRaw` can be leveraged, for instance, for
/// load-balancing or rate-limiting.
pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>,
}

// Dereferencing on the internal [`HashMap`](`Hashmap`) allows users to call all the methods
// implemented on it: `keys()` for one.
impl Deref for Inputs {
    type Target = HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Inputs {
    pub(crate) fn new() -> Self {
        Self {
            hmap: HashMap::default(),
        }
    }

    /// Insert the `flume::Receiver` in the [`Inputs`](`Inputs`), creating the entry if needed in
    /// the internal `HashMap`.
    pub(crate) fn insert(&mut self, port_id: PortId, rx: flume::Receiver<LinkMessage>) {
        self.hmap
            .entry(port_id)
            .or_insert_with(Vec::default)
            .push(rx)
    }

    /// Returns the typed [`Input<T>`](`Input`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Performance
    ///
    /// With a typed [`Input<T>`](`Input`), Zenoh-Flow will perform operations on the underlying
    /// data: if the data are received serialized then Zenoh-Flow will deserialize them, if they are
    /// received "typed" then Zenoh-Flow will check that the type matches what is expected.
    ///
    /// If the underlying data is not relevant then these additional operations can be avoided by
    /// calling `take_raw` and using an [`InputRaw`](`InputRaw`) instead.
    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Input<T>> {
        self.hmap.remove(port_id.as_ref()).map(|receivers| Input {
            _phantom: PhantomData,
            input_raw: InputRaw {
                port_id: port_id.as_ref().into(),
                receivers,
            },
        })
    }

    /// Returns the [`InputRaw`](`InputRaw`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Convenience
    ///
    /// With an [`InputRaw`](`InputRaw`), Zenoh-Flow will not manipulate the underlying data, for
    /// instance trying to deserialize them to a certain `T`.
    ///
    /// It is thus up to the user to call `try_get::<T>()` on the [`Payload`](`Payload`) and handle
    /// the error that could surface.
    ///
    /// If all the data must be "converted" to the same type `T` then calling `take::<T>()` and
    /// using a typed [`Input`](`Input`) will be more convenient and as efficient.
    pub fn take_raw(&mut self, port_id: impl AsRef<str>) -> Option<InputRaw> {
        self.hmap
            .remove(port_id.as_ref())
            .map(|receivers| InputRaw {
                port_id: port_id.as_ref().into(),
                receivers,
            })
    }
}

/// An [`InputRaw`](`InputRaw`) exposes the [`LinkMessage`](`LinkMessage`) it receives.
///
/// It's primary purpose is to ensure "optimal" performance. This can be useful to implement
/// behaviour where actual access to the underlying data is irrelevant.
#[derive(Clone, Debug)]
pub struct InputRaw {
    pub(crate) port_id: PortId,
    pub(crate) receivers: Vec<flume::Receiver<LinkMessage>>,
}

impl InputRaw {
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Input.
    pub fn channels_count(&self) -> usize {
        self.receivers.len()
    }

    /// Returns the first [`LinkMessage`](`LinkMessage`) that was received, *asynchronously*, on any
    /// of the channels associated with this Input.
    ///
    /// If several [`LinkMessage`](`LinkMessage`) are received at the same time, one is *randomly*
    /// selected.
    ///
    /// ## Error
    ///
    /// If an error occurs on one of the channels, this error is returned.
    pub async fn recv_async(&self) -> Result<LinkMessage> {
        let recv_futures = self.receivers.iter().map(|link| link.recv_async());

        let (res, _, _) = futures::future::select_all(recv_futures).await;
        res.map_err(|e| zferror!(ErrorKind::IOError, e).into())
    }
}

#[derive(Clone, Debug)]
pub struct Input<T: ZFData + 'static> {
    _phantom: PhantomData<T>,
    pub(crate) input_raw: InputRaw,
}

// Dereferencing to the [`InputRaw`](`InputRaw`) allows to directly call methods on it with a typed
// [`Input`](`Input`).
impl<T: ZFData + 'static> Deref for Input<T> {
    type Target = InputRaw;

    fn deref(&self) -> &Self::Target {
        &self.input_raw
    }
}

impl<T: ZFData + 'static> Input<T> {
    /// Returns the first [`Message`](`Message`) that was received, *asynchronously*, on any of the
    /// channels associated with this Input.
    ///
    /// If several [`Message`](`Message`) are received at the same time, one is *randomly* selected.
    ///
    /// This method interprets the data to the type associated with this [`Input`](`Input`).
    ///
    /// ## Performance
    ///
    /// As this method interprets the data received additional operations are performed:
    /// - data received serialized is deserialized (an allocation is performed to store an instance
    ///   of `T`),
    /// - data received "typed" are checked against the type associated to this [`Input`](`Input`).
    ///
    /// ## Error
    ///
    /// Several errors can occur:
    /// - one of the channels can return an error (e.g. a disconnection),
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    pub async fn recv_async(&self) -> Result<(Message<T>, Timestamp)> {
        match self.input_raw.recv_async().await? {
            LinkMessage::Data(DataMessage { data, timestamp }) => {
                Ok((Message::Data(Data::try_new(data)?), timestamp))
            }
            LinkMessage::Watermark(timestamp) => Ok((Message::Watermark, timestamp)),
        }
    }
}
