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

use crate::prelude::{ErrorKind, Payload, PortId, ZFData};
use crate::types::{Data, LinkMessage};
use crate::{zferror, Result};
use flume::Sender;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use uhlc::{Timestamp, HLC};

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
    pub(crate) fn new(hlc: Arc<HLC>) -> Self {
        Self {
            hmap: HashMap::default(),
            hlc,
        }
    }

    pub(crate) fn insert(&mut self, port_id: PortId, tx: Sender<LinkMessage>) {
        self.hmap.entry(port_id).or_insert_with(Vec::new).push(tx)
    }

    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Output<T>> {
        self.hmap.remove(port_id.as_ref()).map(|senders| Output {
            _phantom: PhantomData,
            output_raw: OutputRaw {
                port_id: port_id.as_ref().into(),
                senders,
                hlc: Arc::clone(&self.hlc),
                last_watermark: Arc::new(AtomicU64::new(
                    self.hlc.new_timestamp().get_time().as_u64(),
                )),
            },
        })
    }

    pub fn take_raw(&mut self, port_id: impl AsRef<str>) -> Option<OutputRaw> {
        self.hmap.remove(port_id.as_ref()).map(|senders| OutputRaw {
            port_id: port_id.as_ref().into(),
            senders,
            hlc: Arc::clone(&self.hlc),
            last_watermark: Arc::new(AtomicU64::new(self.hlc.new_timestamp().get_time().as_u64())),
        })
    }
}

#[derive(Clone)]
pub struct OutputRaw {
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<LinkMessage>>,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) last_watermark: Arc<AtomicU64>,
}

impl OutputRaw {
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
    pub(crate) fn check_timestamp(&self, timestamp: Option<u64>) -> Result<Timestamp> {
        let ts = match timestamp {
            Some(ts_u64) => Timestamp::new(uhlc::NTP64(ts_u64), *self.hlc.get_id()),
            None => self.hlc.new_timestamp(),
        };

        if ts.get_time().0 < self.last_watermark.load(Ordering::Relaxed) {
            return Err(zferror!(ErrorKind::BelowWatermarkTimestamp(ts)).into());
        }

        Ok(ts)
    }

    pub(crate) async fn send_to_all_async(&self, message: LinkMessage) -> Result<()> {
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

    pub async fn send_async(&self, data: impl Into<Payload>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into(), ts);
        self.send_to_all_async(message).await
    }

    pub async fn forward(&self, message: LinkMessage) -> Result<()> {
        self.send_to_all_async(message).await
    }

    /// Send, *asynchronously*, a watermark on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the watermark on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_watermark_async(&self, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = LinkMessage::Watermark(ts);
        self.send_to_all_async(message).await
    }
}

#[derive(Clone)]
pub struct Output<T: ZFData + 'static> {
    _phantom: PhantomData<T>,
    pub(crate) output_raw: OutputRaw,
}

impl<T: ZFData + 'static> Deref for Output<T> {
    type Target = OutputRaw;

    fn deref(&self) -> &Self::Target {
        &self.output_raw
    }
}

impl<T: ZFData + 'static> Output<T> {
    /// Send, *asynchronously*, the message on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the message on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_async(&self, data: impl Into<Data<T>>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into().into(), ts);
        self.send_to_all_async(message).await
    }
}
