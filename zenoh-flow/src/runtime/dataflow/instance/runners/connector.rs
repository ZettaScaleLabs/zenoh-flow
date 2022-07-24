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
use crate::async_std::sync::{Arc, Mutex};
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::{Runnable, RunnerKind};
use crate::runtime::message::Message;
use crate::runtime::Context;
use crate::{Inputs, NodeId, Outputs, ZFError, ZFResult};
use async_trait::async_trait;
use futures::prelude::*;
use zenoh::net::protocol::io::SplitBuffer;
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
    pub(crate) handle: State,
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
            handle: Ready,
        })
    }

    /// Starts the the sender.
    pub fn start(self) -> ZenohSender<Running> {
        let session = self.context.runtime.session.clone();
        let link = self.link.clone();
        let key_expr = self.key_expr;

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async {
                loop {
                    match link.recv().await {
                        Ok((_, message)) => {
                            let serialized = message.serialize_bincode()?;
                            session
                                .put(key_expr, serialized)
                                .congestion_control(CongestionControl::Block)
                                .await?;
                        }
                        Err(error) => return Err(error),
                    }

                    async_std::task::yield_now().await;
                }
            })
        });

        ZenohSender::<Running> {
            id: self.id,
            context: self.context,
            record: self.record,
            link: self.link.clone(),
            key_expr: self.key_expr,
            handle: Running { handle },
        }
    }

    // /// A single sender iteration
    // ///
    // /// # Errors
    // /// An error variant is returned if:
    // /// - serialization fails
    // /// - zenoh put fails
    // /// - link recv fails
    // async fn iteration(&self) -> ZFResult<()> {
    //     log::debug!("ZenohSender - {} - Started", self.record.resource);
    //     // if let Some(link) = &*self.link.lock().await {
    //     while let Ok((_, message)) = self.link.recv().await {
    //         log::trace!("ZenohSender IN <= {:?} ", message);

    //         let serialized = message.serialize_bincode()?;
    //         log::trace!("ZenohSender - {}=>{:?} ", self.record.resource, serialized);
    //         self.context
    //             .runtime
    //             .session
    //             .put(&self.key_expr, serialized)
    //             .congestion_control(CongestionControl::Block)
    //             .await?;
    //     }
    //     // } else {
    //     //     return Err(ZFError::Disconnected);
    //     // }
    //     Ok(())
    // }
}

impl ZenohSender<Running> {
    pub async fn stop(self) -> ZenohSender<Ready> {
        let Self {
            id,
            context,
            record,
            link,
            key_expr,
            handle,
        }: ZenohSender<Running> = self;

        handle.handle.cancel().await;

        ZenohSender::<Ready> {
            id,
            context,
            record,
            link,
            key_expr,
            handle: Ready,
        }
    }
}

// #[async_trait]
// impl Runnable for ZenohSender {
//     fn get_id(&self) -> NodeId {
//         self.id.clone()
//     }

//     fn get_kind(&self) -> RunnerKind {
//         RunnerKind::Connector
//     }

//     async fn run(&self) -> ZFResult<()> {
//         self.start().await;

//         // Looping on iteration, each iteration is a single
//         // run of the source, as a run can fail in case of error it
//         // stops and returns the error to the caller (the RunnerManager)

//         loop {
//             match self.iteration().await {
//                 Ok(_) => {
//                     log::trace!("[ZenohSender: {}] iteration ok", self.id);
//                     continue;
//                 }
//                 Err(e) => {
//                     log::error!(
//                         "[ZenohSender: {}] iteration failed with error: {}",
//                         self.id,
//                         e
//                     );
//                     self.stop().await;
//                     break Err(e);
//                 }
//             }
//         }
//     }

//     // fn get_outputs(&self) -> HashMap<PortId, PortType> {
//     //     let mut outputs = HashMap::with_capacity(1);
//     //     outputs.insert(
//     //         self.record.link_id.port_id.clone(),
//     //         self.record.link_id.port_type.clone(),
//     //     );
//     //     outputs
//     // }

//     // fn get_inputs(&self) -> HashMap<PortId, PortType> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
//     //     *(self.link.lock().await) = Some(input);
//     //     Ok(())
//     // }

//     // async fn add_output(&self, _output: LinkSender) -> ZFResult<()> {
//     //     Err(ZFError::SenderDoNotHaveOutputs)
//     // }

//     // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
//     //     let mut link_guard = self.link.lock().await;
//     //     if let Some(link) = &*link_guard {
//     //         let mut inputs = HashMap::with_capacity(1);
//     //         inputs.insert(self.record.link_id.port_id.clone(), link.clone());
//     //         *link_guard = None;
//     //         return inputs;
//     //     }
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn start_recording(&self) -> ZFResult<String> {
//     //     Err(ZFError::Unsupported)
//     // }

//     // async fn stop_recording(&self) -> ZFResult<String> {
//     //     Err(ZFError::Unsupported)
//     // }

//     // async fn is_recording(&self) -> bool {
//     //     false
//     // }

//     async fn is_running(&self) -> bool {
//         *self.is_running.lock().await
//     }

//     async fn stop(&self) {
//         self.context
//             .runtime
//             .session
//             .undeclare_expr(self.key_expr)
//             .await
//             .unwrap_or_else(|e| log::warn!("Error when undeclaring expression: {e}"));

//         *self.is_running.lock().await = false;
//     }

//     // async fn clean(self) -> ZFResult<()> {
//     //     Ok(())
//     // }
// }

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running
/// on different runtimes.
#[derive(Clone)]
pub struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) is_running: Arc<Mutex<bool>>,
    pub(crate) key_expr: ExprId,
    // pub(crate) link: Arc<Mutex<Option<LinkSender>>>,
    pub(crate) link: LinkSender,
}

impl ZenohReceiver {
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
            is_running: Arc::new(Mutex::new(false)),
            link,
        })
    }

    /// Starts the receiver.
    async fn start(&self) {
        *self.is_running.lock().await = true;
    }
}

#[async_trait]
impl Runnable for ZenohReceiver {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Connector
    }

    async fn run(&self) -> ZFResult<()> {
        self.start().await;

        let res = {
            log::debug!("ZenohReceiver - {} - Started", self.record.resource);
            // if let Some(link) = &*self.link.lock().await {
            let mut subscriber = self
                .context
                .runtime
                .session
                .subscribe(&self.key_expr)
                .await?;

            while let Some(msg) = subscriber.receiver().next().await {
                log::trace!("ZenohSender - {}<={:?} ", self.record.resource, msg);
                let de: Message = bincode::deserialize(&msg.value.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?;
                log::trace!("ZenohSender - OUT =>{:?} ", de);
                self.link.send(Arc::new(de)).await?;
            }
            // }

            Err(ZFError::Disconnected)
        };

        self.stop().await;
        res
    }

    // fn get_inputs(&self) -> HashMap<PortId, PortType> {
    //     HashMap::with_capacity(0)
    // }

    // fn get_outputs(&self) -> HashMap<PortId, PortType> {
    //     let mut inputs = HashMap::with_capacity(1);
    //     inputs.insert(
    //         self.record.link_id.port_id.clone(),
    //         self.record.link_id.port_type.clone(),
    //     );
    //     inputs
    // }
    // async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
    //     (*self.link.lock().await) = Some(output);
    //     Ok(())
    // }

    // async fn add_input(&self, _input: LinkReceiver) -> ZFResult<()> {
    //     Err(ZFError::ReceiverDoNotHaveInputs)
    // }

    // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
    //     let link_guard = self.link.lock().await;
    //     if let Some(link) = &*link_guard {
    //         let mut outputs = HashMap::with_capacity(1);
    //         outputs.insert(self.record.link_id.port_id.clone(), vec![link.clone()]);
    //         return outputs;
    //     }
    //     HashMap::with_capacity(0)
    // }

    // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
    //     HashMap::with_capacity(0)
    // }

    // async fn clean(self) -> ZFResult<()> {
    //     Ok(())
    // }

    // async fn start_recording(&self) -> ZFResult<String> {
    //     Err(ZFError::Unsupported)
    // }

    // async fn stop_recording(&self) -> ZFResult<String> {
    //     Err(ZFError::Unsupported)
    // }

    // async fn is_recording(&self) -> bool {
    //     false
    // }

    async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    async fn stop(&self) {
        self.context
            .runtime
            .session
            .undeclare_expr(self.key_expr)
            .await
            .unwrap_or_else(|e| log::warn!("Error when undeclaring expression: {e}"));

        *self.is_running.lock().await = false;
    }
}
