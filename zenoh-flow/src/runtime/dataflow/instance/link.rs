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

use crate::{Context, Message, PortId, ZFResult};
use async_std::sync::Arc;
use futures::Future;
use std::{any::Any, pin::Pin};

/// The Zenoh Flow link sender.
/// A wrapper over a flume Sender, that sends `Arc<T>` and is associated
/// with a `PortId`
#[derive(Clone, Debug)]
pub struct LinkSender {
    pub id: PortId,
    pub sender: flume::Sender<Arc<Message>>,
}

pub type CallbackTx =
    Arc<dyn Fn(dyn Any) -> Pin<Box<dyn Future<Output = Message> + Send + Sync>> + Send + Sync>;

pub struct CallbackSender {
    pub(crate) sender: LinkSender,
    pub(crate) callback: CallbackTx,
}

/// The Zenoh Flow link receiver.
/// A wrapper over a flume Receiver, that receives `Arc<T>` and the associated
/// `PortId`
#[derive(Clone, Debug)]
pub struct LinkReceiver {
    pub id: PortId,
    pub receiver: flume::Receiver<Arc<Message>>,
}

pub type CallbackRx = Arc<
    dyn Fn(Arc<dyn Any>, Message) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync,
>;

pub struct CallbackReceiver {
    pub(crate) receiver: LinkReceiver,
    pub(crate) callback: CallbackRx,
}

/// The output of the [`LinkReceiver<T>`](`LinkReceiver<T>`), a tuple
/// containing the `PortId` and `Arc<T>`.
///
/// In Zenoh Flow `T = Data`.
pub type ZFLinkOutput = ZFResult<(PortId, Arc<Message>)>;

impl LinkReceiver {
    /// Wrapper over flume::Receiver::recv_async(),
    /// it returns [`ZFLinkOutput<T>`](`ZFLinkOutput<T>`)
    ///
    /// # Errors
    /// If fails if the link is disconnected
    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput> + '_ + Send + Sync>>
    {
        async fn __recv(_self: &LinkReceiver) -> ZFResult<(PortId, Arc<Message>)> {
            Ok((_self.id.clone(), _self.receiver.recv_async().await?))
        }

        Box::pin(__recv(self))
    }

    pub fn into_callback(self, context: &mut Context, callback: CallbackRx) {
        context.callback_receivers.push(CallbackReceiver {
            receiver: self,
            callback,
        })
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

impl LinkSender {
    /// Wrapper over flume::Sender::send_async(),
    /// it sends `Arc<T>`.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn send(&self, data: Arc<Message>) -> ZFResult<()> {
        Ok(self.sender.send_async(data).await?)
    }

    pub fn into_callback(self, context: &mut Context, callback: CallbackTx) {
        context.callback_senders.push(CallbackSender {
            sender: self,
            callback,
        })
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
pub fn link(
    capacity: Option<usize>,
    send_id: PortId,
    recv_id: PortId,
) -> (LinkSender, LinkReceiver) {
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
