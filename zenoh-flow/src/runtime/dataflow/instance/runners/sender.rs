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
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::link::LinkReceiver;
use crate::runtime::Context;
use crate::{Inputs, NodeId, ZFError, ZFResult};
use zenoh::prelude::*;
use zenoh::publication::CongestionControl;

pub enum SenderRunner {
    Ready(ZenohSender<Ready>),
    Running(ZenohSender<Running>),
    Error(ZenohSender<ZFError>),
}

/// The `ZenohSender` is the connector that sends the data to Zenoh
/// when nodes are running on different runtimes.
pub struct ZenohSender<State> {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) link: LinkReceiver,
    pub(crate) key_expr: ExprId,
    pub(crate) state: State,
}

impl<T> ZenohSender<T> {
    pub fn get_id(&self) -> &NodeId {
        &self.id
    }
}

impl ZenohSender<Ready> {
    /// Creates a new `ZenohSender` with the given parameters.
    ///
    /// # Errors
    /// An error variant is returned if the link is not supposed to be
    /// connected to this node.
    /// Or if the resource declaration in Zenoh fails.
    pub fn try_new(
        context: Context,
        record: ZFConnectorRecord,
        mut inputs: Inputs,
    ) -> ZFResult<Self> {
        let port_id = record.link_id.port_id.clone();

        let mut links = inputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        let link = if links.len() == 1 {
            links.remove(0)
        } else {
            return Err(ZFError::IOError(format!(
                "Expected exactly one link for port < {} > for Zenoh Sender < {} >, found: {}",
                &port_id,
                &record.id,
                links.len()
            )));
        };

        // Declaring the resource to reduce network overhead.
        let key_expr = context
            .runtime
            .session
            .declare_expr(&record.resource)
            .wait()?;

        Ok(Self {
            id: record.id.clone(),
            context,
            record,
            link,
            key_expr,
            state: Ready,
        })
    }

    /// Starts the the sender.
    pub fn start(self) -> ZenohSender<Running> {
        let Self {
            id,
            context,
            record,
            link,
            key_expr,
            state: _,
        } = self;

        let session = context.runtime.session.clone();
        let c_link = link.clone();
        let c_id = id.clone();

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async {
                loop {
                    match c_link.recv().await {
                        Ok((_, message)) => {
                            let serialized = match message.serialize_bincode() {
                                Ok(ser) => ser,
                                Err(e) => return e,
                            };

                            if let Err(e) = session
                                .put(key_expr, serialized)
                                .congestion_control(CongestionControl::Block)
                                .await
                            {
                                return ZFError::ZenohError(format!("{:?}", e));
                            }
                        }
                        Err(error) => {
                            log::error!(
                                "[ZenohSender: (id) {c_id}, (key_expr) {key_expr}] {:?}",
                                error
                            );
                            return error;
                        }
                    }

                    async_std::task::yield_now().await;
                }
            })
        });

        ZenohSender::<Running> {
            id,
            context,
            record,
            link,
            key_expr,
            state: Running { handle },
        }
    }
}

impl ZenohSender<Running> {
    pub fn stop(self) -> ZenohSender<Ready> {
        let Self {
            id,
            context,
            record,
            link,
            key_expr,
            state,
        }: ZenohSender<Running> = self;

        async_std::task::block_on(async { state.handle.cancel().await });
        log::info!("[ZenohSender: (id) {id}, (key_expr) {key_expr}] Stopped");

        ZenohSender::<Ready> {
            id,
            context,
            record,
            link,
            key_expr,
            state: Ready,
        }
    }
}
