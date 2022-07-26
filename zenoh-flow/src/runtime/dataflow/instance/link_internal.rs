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

use crate::{Data, ZFResult};
use async_std::sync::Arc;
use futures::Future;
use std::pin::Pin;

/// The Zenoh Flow link sender.
/// A wrapper over a flume Sender, that sends `Arc<Message>` and is associated
/// with a `PortId`
#[derive(Clone, Debug)]
pub struct LinkSenderInternal {
    pub uids: (u32, u32),
    pub sender: flume::Sender<Arc<Data>>,
}

/// The Zenoh Flow link receiver.
/// A wrapper over a flume Receiver, that receives `Arc<Message>` and the associated
/// `PortId`
#[derive(Clone, Debug)]
pub struct LinkReceiverInternal {
    pub uids: (u32, u32),
    pub receiver: flume::Receiver<Arc<Data>>,
}

/// The output of the [`LinkReceiver`](`LinkReceiver`), a tuple
/// containing the `PortId` and `Arc<Message>`.
///
/// In Zenoh Flow `T = Data`.
///
///
pub type ZFLinkOutput = ZFResult<((u32, u32), Arc<Data>)>;

impl LinkReceiverInternal {
    /// Wrapper over flume::Receiver::recv_async(),
    /// it returns [`ZFLinkOutput`](`ZFLinkOutput`)
    ///
    /// # Errors
    /// If fails if the link is disconnected
    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput> + '_ + Send + Sync>>
    {
        async fn __recv(_self: &LinkReceiverInternal) -> ZFResult<((u32, u32), Arc<Data>)> {
            Ok((_self.uids.clone(), _self.receiver.recv_async().await?))
        }

        Box::pin(__recv(self))
    }

    /// Returns the `PortId` associated with the receiver.
    pub fn uids(&self) -> (u32, u32) {
        self.uids.clone()
    }

    /// Checks if the receiver is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.receiver.is_disconnected()
    }
}

impl LinkSenderInternal {
    /// Wrapper over flume::Sender::send_async(),
    /// it sends `Arc<Data>`.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn send(&self, data: Arc<Data>) -> ZFResult<()> {
        Ok(self.sender.send_async(data).await?)
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
    pub fn uids(&self) -> (u32, u32) {
        self.uids.clone()
    }

    /// Checks is the sender is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.sender.is_disconnected()
    }
}

/// Creates the `Link` with the given capacity and `PortId`s.
pub fn link_internal(
    capacity: Option<usize>,
    send_id: (u32, u32),
    recv_id: (u32, u32),
) -> (LinkSenderInternal, LinkReceiverInternal) {
    let (sender, receiver) = match capacity {
        None => flume::unbounded(),
        Some(cap) => flume::bounded(cap),
    };

    (
        LinkSenderInternal {
            uids: send_id,
            sender,
        },
        LinkReceiverInternal {
            uids: recv_id,
            receiver,
        },
    )
}
