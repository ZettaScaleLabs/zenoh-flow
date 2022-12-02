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

use super::{Data, DataMessage, Message};
use crate::prelude::ZFData;
use crate::types::{LinkMessage, Payload, PortId};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result as ZFResult;
use flume::TryRecvError;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uhlc::{Timestamp, HLC};

pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>,
}

impl Deref for Inputs {
    type Target = HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Inputs {
    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Input<T>> {
        self.hmap.remove(port_id.as_ref()).map(|receivers| Input {
            _phantom: PhantomData,
            port_id: port_id.as_ref().into(),
            receivers,
        })
    }

    pub(crate) fn new() -> Self {
        Self {
            hmap: HashMap::default(),
        }
    }

    pub(crate) fn insert(&mut self, port_id: PortId, rx: flume::Receiver<LinkMessage>) {
        self.hmap
            .entry(port_id)
            .or_insert_with(Vec::default)
            .push(rx)
    }
}

pub struct Outputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Sender<LinkMessage>>>,
    pub(crate) hlc: Arc<HLC>,
}

impl Deref for Outputs {
    type Target = HashMap<PortId, Vec<flume::Sender<LinkMessage>>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Outputs {
    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Output<T>> {
        self.hmap.remove(port_id.as_ref()).map(|senders| Output {
            _phantom: PhantomData,
            port_id: port_id.as_ref().into(),
            hlc: Arc::clone(&self.hlc),
            senders,
            last_watermark: Arc::new(AtomicU64::new(self.hlc.new_timestamp().get_time().as_u64())),
        })
    }

    pub(crate) fn new(hlc: Arc<HLC>) -> Self {
        Self {
            hmap: HashMap::default(),
            hlc,
        }
    }

    pub(crate) fn insert(&mut self, port_id: PortId, tx: flume::Sender<LinkMessage>) {
        self.hmap
            .entry(port_id)
            .or_insert_with(Vec::default)
            .push(tx)
    }
}

#[derive(Clone, Debug)]
pub struct Input<T: ZFData + 'static> {
    _phantom: PhantomData<T>,
    pub(crate) port_id: PortId,
    pub(crate) receivers: Vec<flume::Receiver<LinkMessage>>,
}

impl<T: ZFData + 'static> Input<T> {
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Input.
    pub fn channels_count(&self) -> usize {
        self.receivers.len()
    }

    /// Returns the first `Message` that was received, *asynchronously*, on any of the channels
    /// associated with this Input.
    ///
    /// If several `Message` are received at the same time, one is randomly selected.
    ///
    /// ## Error
    ///
    /// If an error occurs on one of the channels, this error is returned.
    pub async fn recv_async(&self) -> ZFResult<(Message<T>, Timestamp)> {
        let iter = self.receivers.iter().map(|link| link.recv_async());

        // FIXME The remaining futures are not cancelled. Wouldn't a `race` be better in that
        // situation? Or maybe we can store the other futures in the struct and poll them once
        // `recv` is called again?
        let (res, _, _) = futures::future::select_all(iter).await;
        Ok(match res? {
            LinkMessage::Data(DataMessage { data, timestamp }) => {
                (Message::Data(Data::try_new(data)?), timestamp)
            }
            LinkMessage::Watermark(timestamp) => (Message::Watermark, timestamp),
        })
    }

    /// Returns the first `Message` that was received on any of the channels associated with this
    /// Input.
    ///
    /// The order in which the channels are processed match matches the order in which they were
    /// declared in the description file.
    ///
    /// ## Error
    ///
    /// If an error occurs on one of the channel, this error is returned.
    pub fn recv(&self) -> ZFResult<LinkMessage> {
        let mut msg: Option<ZFResult<LinkMessage>> = None;

        while msg.is_none() {
            for receiver in &self.receivers {
                match receiver.try_recv() {
                    Ok(message) => {
                        msg.replace(Ok(message));
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => (),
                        TryRecvError::Disconnected => {
                            msg.replace(Err(zferror!(ErrorKind::Disconnected).into()));
                        }
                    },
                }
            }
        }

        msg.ok_or_else(|| zferror!(ErrorKind::Empty))?
            .map_err(|e| zferror!(ErrorKind::RecvError, "{:?}", e).into())
    }
}

#[derive(Clone)]
pub struct Output<T: ZFData + 'static> {
    _phantom: PhantomData<T>,
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<LinkMessage>>,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) last_watermark: Arc<AtomicU64>,
}

impl<T: ZFData + 'static> Output<T> {
    /// Returns the port id associated with this Output.
    ///
    /// Port ids are unique per type (i.e. Input / Output) and per node.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Output.
    pub fn channels_count(&self) -> usize {
        self.senders.len()
    }

    /// If a timestamp is provided, check that it is not inferior to the latest watermark.
    ///
    /// If no timestamp is provided, a new one is generated from the HLC.
    pub(crate) fn check_timestamp(&self, timestamp: Option<u64>) -> ZFResult<Timestamp> {
        let ts = match timestamp {
            Some(ts_u64) => Timestamp::new(uhlc::NTP64(ts_u64), *self.hlc.get_id()),
            None => self.hlc.new_timestamp(),
        };

        if ts.get_time().0 < self.last_watermark.load(Ordering::Relaxed) {
            return Err(zferror!(ErrorKind::BelowWatermarkTimestamp(ts)).into());
        }

        Ok(ts)
    }

    pub(crate) fn send_to_all(&self, message: LinkMessage) -> ZFResult<()> {
        // FIXME Feels like a cheap hack counting the number of errors. To improve.
        let mut err = 0usize;
        for sender in &self.senders {
            if let Err(e) = sender.send(message.clone()) {
                log::error!("[Output: {}] {:?}", self.port_id, e);
                err += 1;
            }
        }

        if err > 0 {
            return Err(zferror!(
                ErrorKind::SendError,
                "[Output: {}] Encountered {} errors while sending (or trying to)",
                self.port_id,
                err
            )
            .into());
        }

        Ok(())
    }

    pub(crate) async fn send_to_all_async(&self, message: LinkMessage) -> ZFResult<()> {
        // FIXME Feels like a cheap hack counting the number of errors. To improve.
        let mut err = false;
        let fut_senders = self
            .senders
            .iter()
            .map(|sender| sender.send_async(message.clone()));
        let res = futures::future::join_all(fut_senders).await;

        res.iter().for_each(|res| {
            if let Err(e) = res {
                log::error!(
                    "[Output: {}] Error occured while sending to downstream node(s): {:?}",
                    self.port_id(),
                    e
                );
                err = true;
            }
        });

        if err {
            return Err(zferror!(
                ErrorKind::SendError,
                "[Output: {}] Encountered {} errors while async sending (or trying to)",
                self.port_id,
                err
            )
            .into());
        }

        Ok(())
    }

    /// Send, *synchronously*, the message on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the message on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub fn send(&self, data: impl Into<Payload>, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into(), ts);
        self.send_to_all(message)
    }

    /// Send, *asynchronously*, the message on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the message on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_async(
        &self,
        data: impl Into<Data<T>>,
        timestamp: Option<u64>,
    ) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into().into(), ts);
        self.send_to_all_async(message).await
    }

    /// Send, *synchronously*, a watermark on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the watermark on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub fn send_watermark(&self, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = LinkMessage::Watermark(ts);
        self.send_to_all(message)
    }

    /// Send, *asynchronously*, a watermark on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the watermark on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_watermark_async(&self, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = LinkMessage::Watermark(ts);
        self.send_to_all_async(message).await
    }
}
