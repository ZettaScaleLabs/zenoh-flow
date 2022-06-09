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

use crate::{PortId, ZFResult};
use async_std::sync::Arc;
use futures::Future;
use std::pin::Pin;

/// The Zenoh Flow link sender.
/// A wrapper over a flume Sender, that sends `Arc<T>` and is associated
/// with a `PortId`
#[derive(Clone, Debug)]
pub struct LinkSender<T> {
    pub id: PortId,
    pub sender: flume::Sender<Arc<T>>,
}

pub type CallbackTx<State, T> =
    Arc<dyn Fn(State) -> Pin<Box<dyn Future<Output = T> + Send + Sync>>>;
pub struct CallbackSender<State, T> {
    pub(crate) sender: LinkSender<T>,
    pub(crate) callback: CallbackTx<State, T>,
}

/// The Zenoh Flow link receiver.
/// A wrapper over a flume Receiver, that receives `Arc<T>` and the associated
/// `PortId`
#[derive(Clone, Debug)]
pub struct LinkReceiver<T> {
    pub id: PortId,
    pub receiver: flume::Receiver<Arc<T>>,
}

pub type CallbackRx<State, T> =
    Arc<dyn Fn(State, T) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>>;
pub struct CallbackReceiver<State, T> {
    pub(crate) receiver: LinkReceiver<T>,
    pub(crate) callback: CallbackRx<State, T>,
}

/// The output of the [`LinkReceiver<T>`](`LinkReceiver<T>`), a tuple
/// containing the `PortId` and `Arc<T>`.
///
/// In Zenoh Flow `T = Data`.
pub type ZFLinkOutput<T> = ZFResult<(PortId, Arc<T>)>;

impl<T: 'static + Send + Sync> LinkReceiver<T> {
    /// Wrapper over flume::Receiver::recv_async(),
    /// it returns [`ZFLinkOutput<T>`](`ZFLinkOutput<T>`)
    ///
    /// # Errors
    /// If fails if the link is disconnected
    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_ + Send + Sync>>
    {
        async fn __recv<T>(_self: &LinkReceiver<T>) -> ZFResult<(PortId, Arc<T>)> {
            Ok((_self.id.clone(), _self.receiver.recv_async().await?))
        }

        Box::pin(__recv(self))
    }

    pub fn into_callback<State: 'static + Send + Sync>(
        self,
        callback: CallbackRx<State, T>,
    ) -> CallbackReceiver<State, T> {
        CallbackReceiver {
            receiver: self,
            callback,
        }
    }

    /// Discards the message
    ///
    /// *Note:* Not implemented.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn discard(&self) -> ZFResult<()> {
        Ok(())
    }

    /// Returns the `PortId` associated with the receiver.
    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    /// Checks if the receiver is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.receiver.is_disconnected()
    }
}

impl<T: 'static + Send + Sync> LinkSender<T> {
    /// Wrapper over flume::Sender::send_async(),
    /// it sends `Arc<T>`.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn send(&self, data: Arc<T>) -> ZFResult<()> {
        Ok(self.sender.send_async(data).await?)
    }

    pub fn into_callback<State: 'static + Send + Sync>(
        self,
        context: &mut Context, // FIXME
        callback: CallbackTx<State, T>,
    ) -> CallbackSender<State, T> {
        CallbackSender {
            sender: self,
            callback,
        }
    }

    /// Returns the sender occupation.
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Checks if the sender is empty.
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Returns the sender capacity if any.
    pub fn capacity(&self) -> Option<usize> {
        self.sender.capacity()
    }

    /// Returns the sender `PortId`.
    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    /// Checks is the sender is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.sender.is_disconnected()
    }
}

/// Creates the `Link` with the given capacity and `PortId`s.
pub fn link<T>(
    capacity: Option<usize>,
    send_id: PortId,
    recv_id: PortId,
) -> (LinkSender<T>, LinkReceiver<T>) {
    let (sender, receiver) = match capacity {
        None => flume::unbounded(),
        Some(cap) => flume::bounded(cap),
    };

    (
        LinkSender {
            id: send_id,
            sender,
        },
        LinkReceiver {
            id: recv_id,
            receiver,
        },
    )
}
