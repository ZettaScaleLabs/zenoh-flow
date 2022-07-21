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

use super::{Ready, Running};
use crate::async_std::sync::Arc;
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::link::LinkSender;
use crate::runtime::Context;
use crate::{NodeId, Outputs, ZFError, ZFResult};
use futures::prelude::*;
use zenoh::net::protocol::io::SplitBuffer;
use zenoh::prelude::*;

pub enum ReceiverRunner {
    Ready(ZenohReceiver<Ready>),
    Running(ZenohReceiver<Running>),
}

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running
/// on different runtimes.
#[derive(Clone)]
pub struct ZenohReceiver<State> {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) key_expr: ExprId,
    pub(crate) link: LinkSender,
    pub(crate) state: State,
}

impl<T> ZenohReceiver<T> {
    pub fn get_id(&self) -> NodeId {
        self.id.clone()
    }
}

impl ZenohReceiver<Ready> {
    /// Creates a new `ZenohReceiver` with the given parametes.
    ///
    /// # Errors
    /// An error variant is returned if the link is not supposed to be
    /// connected to this node.
    pub fn try_new(
        context: Context,
        record: ZFConnectorRecord,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {
        let port_id = record.link_id.port_id.clone();
        let mut links = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        let link = if links.len() == 1 {
            links.remove(0)
        } else {
            return Err(ZFError::IOError(format!(
                "Expected exactly one link for port < {} > for Zenoh Receiver < {} >, found: {}",
                &port_id,
                &record.id,
                links.len()
            )));
        };

        let key_expr = context
            .runtime
            .session
            .declare_expr(&record.resource)
            .wait()?;

        Ok(Self {
            id: record.id.clone(),
            context,
            record,
            key_expr,
            link,
            state: Ready,
        })
    }

    /// Starts the receiver.
    pub fn start(self) -> ZenohReceiver<Running> {
        let Self {
            id,
            context,
            record,
            key_expr,
            link,
            state: _,
        } = self;

        let session = context.runtime.session.clone();
        let c_id = id.clone();
        let c_link = link.clone();

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async {
                let mut subscriber = match session.subscribe(&key_expr).await {
                    Ok(sub) => sub,
                    Err(e) => {
                        log::error!("[ZenohReceiver (id) {c_id} (key_expr) {key_expr}] {:?}", e);
                        return ZFError::ZenohError(format!("{:?}", e));
                    }
                };

                loop {
                    match subscriber.receiver().next().await {
                        Some(ser_message) => {
                            let message =
                                match bincode::deserialize(&ser_message.value.payload.contiguous())
                                {
                                    Ok(message) => message,
                                    Err(e) => {
                                        log::error!(
                                        "[ZenohReceiver (id) {c_id} (key_expr) {key_expr}] {:?}",
                                        e
                                    );
                                        return ZFError::DeseralizationError;
                                    }
                                };

                            if let Err(e) = c_link.send(Arc::new(message)).await {
                                log::error!(
                                    "[ZenohReceiver (id) {c_id} (key_expr) {key_expr}] {:?}",
                                    e
                                );
                                return ZFError::DeseralizationError;
                            }
                        }

                        None => {
                            log::error!(
                                "[ZenohReceiver (id) {c_id} (key_expr) {key_expr}] Disconnected from Zenoh",
                            );
                            return ZFError::Disconnected;
                        }
                    }

                    async_std::task::yield_now().await;
                }
            })
        });

        ZenohReceiver::<Running> {
            id,
            context,
            record,
            link,
            key_expr,
            state: Running { handle },
        }
    }
}

impl ZenohReceiver<Running> {
    pub fn stop(self) -> ZenohReceiver<Ready> {
        let Self {
            id,
            context,
            record,
            key_expr,
            link,
            state,
        } = self;

        async_std::task::block_on(async { state.handle.cancel().await });
        log::info!("[ZenohReceiver (id) {id} (key_expr) {key_expr}] Stopped");

        ZenohReceiver::<Ready> {
            id,
            context,
            record,
            key_expr,
            link,
            state: Ready,
        }
    }
}
