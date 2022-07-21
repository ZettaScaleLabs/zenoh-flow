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

pub mod link;
pub mod runners;

use self::runners::operator::Operator;
use self::runners::sink::Sink;
use self::runners::source::Source;
use self::runners::Ready;
use crate::model::connector::ZFConnectorKind;
use crate::model::link::LinkRecord;
use crate::model::NodeKind;
use crate::runtime::dataflow::instance::link::link;
use crate::runtime::dataflow::instance::runners::operator::OperatorRunner;
use crate::runtime::dataflow::instance::runners::receiver::{ReceiverRunner, ZenohReceiver};
use crate::runtime::dataflow::instance::runners::sender::{SenderRunner, ZenohSender};
use crate::runtime::dataflow::instance::runners::sink::SinkRunner;
use crate::runtime::dataflow::instance::runners::source::SourceRunner;
use crate::runtime::dataflow::Dataflow;
use crate::runtime::Context;
use crate::{Inputs, NodeId, Outputs, /* PortId, PortType, */ ZFError, ZFResult};
use async_std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

/// The instance of a data flow graph.
/// It contains runtime information for the instance
/// and the [`InstanceContext`](`InstanceContext`)
pub struct DataflowInstance {
    pub(crate) context: Context,
    pub(crate) runners: HashMap<NodeId, NodeKind>,
    pub(crate) source_runners: HashMap<NodeId, SourceRunner>,
    pub(crate) operator_runners: HashMap<NodeId, OperatorRunner>,
    pub(crate) sink_runners: HashMap<NodeId, SinkRunner>,
    pub(crate) receiver_runners: HashMap<NodeId, ReceiverRunner>,
    pub(crate) sender_runners: HashMap<NodeId, SenderRunner>,
}

/// Creates the [`Link`](`Link`) between the `nodes` using `links`.
///
/// # Errors
/// An error variant is returned in case of:
/// -  port id is duplicated.
fn create_links(
    nodes: &[NodeId],
    links: &[LinkRecord],
) -> ZFResult<HashMap<NodeId, (Inputs, Outputs)>> {
    let mut io: HashMap<NodeId, (Inputs, Outputs)> = HashMap::with_capacity(nodes.len());

    for link_desc in links {
        let upstream_node = link_desc.from.node.clone();
        let downstream_node = link_desc.to.node.clone();

        // Nodes have been filtered based on their runtime. If the runtime of either one of the node
        // is not equal to that of the current runtime, the channels should not be created.
        if !nodes.contains(&upstream_node) || !nodes.contains(&downstream_node) {
            continue;
        }

        let (tx, rx) = link(
            None,
            link_desc.from.output.clone(),
            link_desc.to.input.clone(),
        );

        match io.get_mut(&upstream_node) {
            Some((_, outputs)) => outputs.add(tx),
            None => {
                let mut outputs = Outputs::new();
                let inputs = Inputs::new();
                outputs.add(tx);
                io.insert(upstream_node, (inputs, outputs));
            }
        }

        match io.get_mut(&downstream_node) {
            Some((inputs, _)) => inputs.add(rx),
            None => {
                let outputs = Outputs::new();
                let mut inputs = Inputs::new();
                inputs.add(rx);
                io.insert(downstream_node, (inputs, outputs));
            }
        }
    }

    Ok(io)
}

impl DataflowInstance {
    /// Tries to instantiate the [`Dataflow`](`Dataflow`)
    ///
    /// This function is called by the runtime once the `Dataflow` object was
    /// created and validated.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - validation fails
    /// - connectors cannot be created
    pub fn try_instantiate(dataflow: Dataflow) -> ZFResult<Self> {
        // Gather all node ids to be able to generate (i) the links and (ii) the hash map containing
        // the runners.
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(
            dataflow.sources.len()
                + dataflow.operators.len()
                + dataflow.sinks.len()
                + dataflow.connectors.len(),
        );

        node_ids.append(&mut dataflow.sources.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.operators.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.sinks.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.connectors.keys().cloned().collect::<Vec<_>>());

        let mut links = create_links(&node_ids, &dataflow.links)?;
        let mut runners = HashMap::with_capacity(node_ids.len());

        let context = Context {
            flow_id: dataflow.flow_id,
            instance_id: dataflow.uuid,
            runtime: dataflow.context,
            // callback_receivers: Vec::new(),
            // callback_senders: Vec::new(),
        };

        let mut source_runners = HashMap::with_capacity(dataflow.sources.len());

        for (id, source) in dataflow.sources.into_iter() {
            let (_, outputs) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Source < {} > were not created.",
                    &source.id
                ))
            })?;

            runners.insert(id.clone(), NodeKind::Source);

            source_runners.insert(
                id,
                SourceRunner::Ready(Source::<Ready>::new(context.clone(), source, outputs)),
            );
        }

        let mut operator_runners = HashMap::with_capacity(dataflow.operators.len());

        for (id, operator) in dataflow.operators.into_iter() {
            let io = links.remove(&operator.id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Operator < {} > were not created.",
                    &operator.id
                ))
            })?;

            runners.insert(id.clone(), NodeKind::Operator);

            operator_runners.insert(
                id,
                OperatorRunner::Ready(Operator::<Ready>::new(context.clone(), operator, io)),
            );
        }

        let mut sink_runners = HashMap::with_capacity(dataflow.sinks.len());

        for (id, sink) in dataflow.sinks.into_iter() {
            let (inputs, _) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!("Links for Sink < {} > were not created.", &sink.id))
            })?;

            runners.insert(id.clone(), NodeKind::Sink);

            sink_runners.insert(
                id,
                SinkRunner::Ready(Sink::<Ready>::new(context.clone(), sink, inputs)),
            );
        }

        let mut sender_runners = HashMap::with_capacity(dataflow.connectors.len() / 2);
        let mut receiver_runners = HashMap::with_capacity(dataflow.connectors.len() / 2);

        for (id, connector) in dataflow.connectors.into_iter() {
            let (inputs, outputs) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Connector < {} > were not created.",
                    &connector.id
                ))
            })?;
            match connector.kind {
                ZFConnectorKind::Sender => {
                    sender_runners.insert(
                        id.clone(),
                        SenderRunner::Ready(ZenohSender::<Ready>::try_new(
                            context.clone(),
                            connector,
                            inputs,
                        )?),
                    );

                    runners.insert(id, NodeKind::Sender);
                }
                ZFConnectorKind::Receiver => {
                    receiver_runners.insert(
                        id.clone(),
                        ReceiverRunner::Ready(ZenohReceiver::<Ready>::try_new(
                            context.clone(),
                            connector,
                            outputs,
                        )?),
                    );

                    runners.insert(id, NodeKind::Receiver);
                }
            }
        }

        Ok(Self {
            context,
            runners,
            source_runners,
            operator_runners,
            sink_runners,
            receiver_runners,
            sender_runners,
        })
    }

    /// Returns the instance's `Uuid`.
    pub fn get_uuid(&self) -> Uuid {
        self.context.instance_id
    }

    /// Returns the instance's `FlowId`.
    pub fn get_flow(&self) -> Arc<str> {
        self.context.flow_id.clone()
    }

    /// Returns a copy of the `InstanceContext`.
    pub fn get_instance_context(&self) -> Context {
        self.context.clone()
    }

    /// Returns the `NodeId` for all the sources in this instance.
    pub fn get_sources(&self) -> impl Iterator<Item = &NodeId> + '_ {
        // self.runners
        //     .values()
        //     .filter(|runner| matches!(runner.get_kind(), RunnerKind::Source))
        //     .map(|runner| runner.get_id())
        //     .collect()
        self.source_runners.keys()
    }

    /// Returns the `NodeId` for all the sinks in this instance.
    pub fn get_sinks(&self) -> impl Iterator<Item = &NodeId> + '_ {
        // self.runners
        //     .values()
        //     .filter(|runner| matches!(runner.get_kind(), RunnerKind::Sink))
        //     .map(|runner| runner.get_id())
        //     .collect()
        self.sink_runners.keys()
    }

    /// Returns the `NodeId` for all the sinks in this instance.
    pub fn get_operators(&self) -> impl Iterator<Item = &NodeId> + '_ {
        // self.runners
        //     .values()
        //     .filter(|runner| matches!(runner.get_kind(), RunnerKind::Operator))
        //     .map(|runner| runner.get_id())
        //     .collect()
        self.operator_runners.keys()
    }

    /// Returns the `NodeId` for all the connectors in this instance.
    pub fn get_connectors(&self) -> Vec<NodeId> {
        // self.runners
        //     .values()
        //     .filter(|runner| matches!(runner.get_kind(), RunnerKind::Connector))
        //     .map(|runner| runner.get_id())
        //     .collect()
        unimplemented!()
    }

    /// Returns the `NodeId` for all the nodes in this instance.
    pub fn get_nodes(&self) -> impl Iterator<Item = &NodeId> + '_ {
        // self.runners
        //     .values()
        //     .map(|runner| runner.get_id())
        //     .collect()
        self.runners.keys()
    }

    // /// Starts all the sources in this instance.
    // ///
    // ///
    // /// **Note:** Not implemented.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// -  sources already started.
    // pub async fn start_sources(&mut self) -> ZFResult<()> {
    //     Err(ZFError::Unimplemented)
    // }

    // /// Starts all the nodes in this instance.
    // ///
    // /// **Note:** Not implemented.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// -  nodes already started.
    // pub async fn start_nodes(&mut self) -> ZFResult<()> {
    //     Err(ZFError::Unimplemented)
    // }

    // /// Stops all the sources in this instance.
    // ///
    // /// **Note:** Not implemented.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// -  sources already stopped.
    // pub async fn stop_sources(&mut self) -> ZFResult<()> {
    //     Err(ZFError::Unimplemented)
    // }

    // /// Stops all the sources in this instance.
    // ///
    // /// **Note:** Not implemented.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// -  nodes already stopped.
    // pub async fn stop_nodes(&mut self) -> ZFResult<()> {
    //     Err(ZFError::Unimplemented)
    // }

    /// Checks if the given node is running.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub fn is_node_running(&self, node_id: &NodeId) -> ZFResult<bool> {
        if let Some(kind) = self.runners.get(node_id) {
            match kind {
                NodeKind::Operator => {
                    if let Some(operator) = self.operator_runners.get(node_id) {
                        return Ok(matches!(operator, OperatorRunner::Running(_)));
                    }
                }
                NodeKind::Source => {
                    if let Some(source) = self.source_runners.get(node_id) {
                        return Ok(matches!(source, SourceRunner::Running(_)));
                    }
                }
                NodeKind::Sink => {
                    if let Some(sink) = self.sink_runners.get(node_id) {
                        return Ok(matches!(sink, SinkRunner::Running(_)));
                    }
                }
                NodeKind::Sender => {
                    if let Some(sender) = self.sender_runners.get(node_id) {
                        return Ok(matches!(sender, SenderRunner::Running(_)));
                    }
                }
                NodeKind::Receiver => {
                    if let Some(receiver) = self.receiver_runners.get(node_id) {
                        return Ok(matches!(receiver, ReceiverRunner::Running(_)));
                    }
                }
            }
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    /// Starts the given node.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn start_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
        if let Some(kind) = self.runners.get(node_id) {
            match kind {
                NodeKind::Source => {
                    if let Some(source) = self.source_runners.remove(node_id) {
                        let op = match source {
                            SourceRunner::Ready(source) => source.start(),
                            SourceRunner::Running(source) => {
                                log::warn!(
                                    "[Source: (id) {node_id}] Cannot start already running source"
                                );
                                source
                            }
                        };
                        self.source_runners
                            .insert(node_id.clone(), SourceRunner::Running(op));
                        return Ok(());
                    }
                }

                NodeKind::Operator => {
                    if let Some(operator) = self.operator_runners.remove(node_id) {
                        let op = match operator {
                            OperatorRunner::Ready(operator) => operator.start(),
                            OperatorRunner::Running(operator) => {
                                log::warn!("[Operator: (id) {node_id}] Cannot start already running operator");
                                operator
                            }
                        };
                        self.operator_runners
                            .insert(node_id.clone(), OperatorRunner::Running(op));
                        return Ok(());
                    }
                }

                NodeKind::Sink => {
                    if let Some(sink) = self.sink_runners.remove(node_id) {
                        let op = match sink {
                            SinkRunner::Ready(sink) => sink.start(),
                            SinkRunner::Running(sink) => {
                                log::warn!(
                                    "[Sink: (id) {node_id}] Cannot start already running sink"
                                );
                                sink
                            }
                        };
                        self.sink_runners
                            .insert(node_id.clone(), SinkRunner::Running(op));
                        return Ok(());
                    }
                }

                _ => {
                    log::warn!("Only Source, Operator and Sink nodes can be started manually.");
                    return Ok(());
                }
            }
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    /// Stops the given node.
    ///
    /// # Errors
    /// If fails if the node is not found or it is not running.
    pub async fn stop_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
        if let Some(kind) = self.runners.get(node_id) {
            match kind {
                NodeKind::Source => {
                    if let Some(source) = self.source_runners.remove(node_id) {
                        let source_ready = match source {
                            SourceRunner::Ready(source) => {
                                log::warn!(
                                    "[Source: (id) {node_id}] Cannot stop already stopped source"
                                );
                                source
                            }
                            SourceRunner::Running(source) => source.stop(),
                        };
                        self.source_runners
                            .insert(node_id.clone(), SourceRunner::Ready(source_ready));
                        return Ok(());
                    }
                }

                NodeKind::Operator => {
                    if let Some(operator) = self.operator_runners.remove(node_id) {
                        let operator_ready = match operator {
                            OperatorRunner::Ready(operator) => {
                                log::warn!(
                                    "[Operator: (id) {node_id}] Cannot stop already stopped operator"
                                );
                                operator
                            }
                            OperatorRunner::Running(operator) => operator.stop(),
                        };
                        self.operator_runners
                            .insert(node_id.clone(), OperatorRunner::Ready(operator_ready));
                        return Ok(());
                    }
                }

                NodeKind::Sink => {
                    if let Some(sink) = self.sink_runners.remove(node_id) {
                        let sink_ready = match sink {
                            SinkRunner::Ready(sink) => {
                                log::warn!(
                                    "[Sink: (id) {node_id}] Cannot stop already stopped sink"
                                );
                                sink
                            }
                            SinkRunner::Running(sink) => sink.stop(),
                        };
                        self.sink_runners
                            .insert(node_id.clone(), SinkRunner::Ready(sink_ready));
                        return Ok(());
                    }
                }

                _ => {
                    log::warn!("Only Source, Operator and Sink nodes can be stopped manually.");
                    return Ok(());
                }
            }
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    // /// Finalized the given node.
    // /// Finalizing a node means cleaning up its state.
    // ///
    // /// # Errors
    // /// If fails if the node is not found.
    // pub async fn clean_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
    //     // let runner = self
    //     //     .runners
    //     //     .get(node_id)
    //     //     .ok_or_else(|| ZFError::NodeNotFound(node_id.clone()))?;
    //     // runner.clean().await
    //     Ok(())
    // }

    /// Starts the recording for the given source.
    ///
    /// It returns the key expression where the recording is stored.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn start_recording(&self, node_id: &NodeId) -> ZFResult<String> {
        // let manager = self
        //     .managers
        //     .get(node_id)
        //     .ok_or_else(|| ZFError::NodeNotFound(node_id.clone()))?;
        // manager.start_recording().await

        Ok("recording in progress".to_string())
    }

    /// Stops the recording for the given source.
    ///
    /// It returns the key expression where the recording is stored.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn stop_recording(&self, node_id: &NodeId) -> ZFResult<String> {
        // let manager = self
        //     .managers
        //     .get(node_id)
        //     .ok_or_else(|| ZFError::NodeNotFound(node_id.clone()))?;
        // manager.stop_recording().await
        Ok("recording stopped".to_string())
    }

    /// Assumes the source is already stopped before calling the start replay!
    /// This method is called by the runtime, that always check that the node
    /// is not running prior to call this function.
    /// If someone is using directly the DataflowInstance need to stop and check
    /// if the node is running before calling this function.
    ///
    /// # Errors
    /// It fails if:
    /// - the source is not stopped
    /// - the node is not a source
    /// - the key expression is not point to a recording
    pub async fn start_replay(&mut self, source_id: &NodeId, resource: String) -> ZFResult<NodeId> {
        // let runner = self
        //     .runners
        //     .get(source_id)
        //     .ok_or_else(|| ZFError::NodeNotFound(source_id.clone()))?;

        // let mut outputs: Vec<(PortId, PortType)> = runner
        //     .get_outputs()
        //     .iter()
        //     .map(|(k, v)| (k.clone(), v.clone()))
        //     .collect();

        // let (output_id, _output_type) = outputs
        //     .pop()
        //     .ok_or_else(|| ZFError::NodeNotFound(source_id.clone()))?;

        // let replay_id: NodeId = format!(
        //     "replay-{}-{}-{}-{}",
        //     self.context.flow_id, self.context.instance_id, source_id, output_id
        // )
        // .into();

        // let output_links = runner
        //     .get_outputs_links()
        //     .await
        //     .remove(&output_id)
        //     .ok_or_else(|| ZFError::PortNotFound((source_id.clone(), output_id.clone())))?;

        // let replay_node = ZenohReplay::try_new(
        //     replay_id.clone(),
        //     self.context.clone(),
        //     source_id.clone(),
        //     output_id,
        //     _output_type,
        //     output_links,
        //     resource,
        // )?;

        // let replay_runner = NodeRunner::new(Arc::new(replay_node), self.context.clone());
        // let replay_manager = replay_runner.start();

        // self.runners.insert(replay_id.clone(), replay_runner);
        // self.managers.insert(replay_id.clone(), replay_manager);
        // Ok(replay_id)
        Ok(source_id.clone())
    }

    /// Stops the recording for the given source.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn stop_replay(&mut self, replay_id: &NodeId) -> ZFResult<()> {
        self.stop_node(replay_id).await?;
        self.runners.remove(replay_id);
        Ok(())
    }
}
