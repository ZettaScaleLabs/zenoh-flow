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
pub mod link_internal;
pub mod runners;

use crate::model::connector::ZFConnectorKind;
use crate::model::link::LinkRecord;
use crate::runtime::dataflow::instance::link::{link, LinkSender};
use crate::runtime::dataflow::instance::runners::connector::{ZenohReceiver, ZenohSender};
use crate::runtime::dataflow::instance::runners::operator::OperatorRunner;
use crate::runtime::dataflow::instance::runners::sink::SinkRunner;
use crate::runtime::dataflow::instance::runners::source::SourceRunner;
use crate::runtime::dataflow::instance::runners::RunnerKind;
use crate::runtime::dataflow::Dataflow;
use crate::runtime::InstanceContext;
use crate::{Inputs, Message, NodeId, Outputs, PortId, ZFError, ZFResult};
use async_std::sync::Arc;

use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::Directed;
use std::collections::HashMap;
use std::convert::TryFrom;
use uhlc::{Timestamp, ID};
use uuid::Uuid;

use self::link_internal::LinkReceiverInternal;
use self::runners::Runner;
use futures::future::{AbortHandle, Abortable, Aborted};
use futures::{future, Future};
use petgraph::stable_graph::StableDiGraph;
use petgraph::stable_graph::StableGraph;
use async_std::task::JoinHandle;

/// The instance of a data flow graph.
/// It contains runtime information for the instance
/// and the [`InstanceContext`](`InstanceContext`)
pub struct DataflowInstance {
    pub(crate) context: InstanceContext,
    pub(crate) runners: HashMap<NodeId, Box<dyn Runner>>,
    pub(crate) graph: StableDiGraph<u32, (u32, u32)>,
    pub(crate) abort_handle: AbortHandle,
    pub(crate) join_handle: JoinHandle<Result<(), Aborted>>,
}

/// Creates the [`Link`](`Link`) between the `nodes` using `links`.
///
/// # Errors
/// An error variant is returned in case of:
/// -  port id is duplicated.
fn create_links(
    nodes: &[NodeId],
    links: &[LinkRecord],
    nodes_ports_uids: &HashMap<NodeId, (u32, HashMap<PortId, u32>, HashMap<PortId, u32>)>,
    graph: &mut StableDiGraph<u32, (u32, u32)>,
    node_indexes: &HashMap<u32, NodeIndex<u32>>,
) -> ZFResult<(
    HashMap<NodeId, (Inputs, Outputs)>,
    HashMap<(u32, u32), Vec<LinkSender>>,
    HashMap<(u32, u32), LinkReceiverInternal>,
)> {
    let mut io: HashMap<NodeId, (Inputs, Outputs)> = HashMap::with_capacity(nodes.len());
    let mut lookup_table: HashMap<(u32, u32), Vec<(u32, u32)>> = HashMap::new();
    let mut lookup_table_ch: HashMap<(u32, u32), Vec<LinkSender>> = HashMap::new();
    let mut receivers: HashMap<(u32, u32), LinkReceiverInternal> = HashMap::new();

    for link_desc in links {
        let upstream_node = link_desc.from.node.clone();
        let downstream_node = link_desc.to.node.clone();
        let upstream_port = link_desc.from.output.clone();
        let downstream_port = link_desc.to.input.clone();

        // Nodes have been filtered based on their runtime. If the runtime of either one of the node
        // is not equal to that of the current runtime, the channels should not be created.
        if !nodes.contains(&upstream_node) || !nodes.contains(&downstream_node) {
            continue;
        }

        // Getting UIDs and NodeIndexes, creating the graph in petgraph

        let (sender_uid, _, outputs) = nodes_ports_uids
            .get(&upstream_node)
            .ok_or(ZFError::NodeNotFound(upstream_node.clone()))?;
        let sender_port_uid = outputs.get(&upstream_port).ok_or(ZFError::PortNotFound((
            upstream_node.clone(),
            upstream_port.clone(),
        )))?;

        let (receiver_uid, inputs, _) = nodes_ports_uids
            .get(&downstream_node)
            .ok_or(ZFError::NodeNotFound(downstream_node.clone()))?;
        let receiver_port_uid = inputs.get(&downstream_port).ok_or(ZFError::PortNotFound((
            downstream_node.clone(),
            downstream_port.clone(),
        )))?;

        let upstream_index = node_indexes
            .get(sender_uid)
            .ok_or(ZFError::NodeNotFound(upstream_node.clone()))?;
        let downstream_index = node_indexes
            .get(receiver_uid)
            .ok_or(ZFError::NodeNotFound(downstream_node.clone()))?;

        log::error!("({sender_uid}:{sender_port_uid})=>({receiver_uid}:{receiver_port_uid}) | ({upstream_node}:{upstream_port}) => ({downstream_node}:{downstream_port})");

        graph.add_edge(
            *upstream_index,
            *downstream_index,
            (*sender_port_uid, *receiver_port_uid),
        );

        let (itx, urx) = link(None, upstream_port.clone(), downstream_port.clone());

        // lookup table update
        match lookup_table.get_mut(&(*sender_uid, *sender_port_uid)) {
            Some(v) => {
                v.push((*receiver_uid, *receiver_port_uid));
            }
            None => {
                let faces = vec![(*receiver_uid, *receiver_port_uid)];
                lookup_table.insert((*sender_uid, *sender_port_uid), faces);
            }
        };

        match lookup_table_ch.get_mut(&(*sender_uid, *sender_port_uid)) {
            Some(v) => {
                v.push(itx);
            }
            None => {
                let txs = vec![itx];
                lookup_table_ch.insert((*sender_uid, *sender_port_uid), txs);
            }
        };

        log::error!("Lookup table {lookup_table:?}");
        log::error!("Lookup table (Channels) {lookup_table_ch:?}");

        //

        let (utx, irx) = link_internal::link_internal(
            None,
            (*sender_uid, *sender_port_uid),
            (*sender_uid, *sender_port_uid),
        );

        receivers.insert((*sender_uid, *sender_port_uid), irx);

        match io.get_mut(&upstream_node) {
            Some((_, outputs)) => outputs.add(upstream_port, utx),
            None => {
                let mut outputs = Outputs::new();
                let inputs = Inputs::new();
                outputs.add(upstream_port, utx);
                io.insert(upstream_node, (inputs, outputs));
            }
        }

        match io.get_mut(&downstream_node) {
            Some((inputs, _)) => inputs.add(urx),
            None => {
                let outputs = Outputs::new();
                let mut inputs = Inputs::new();
                inputs.add(urx);
                io.insert(downstream_node, (inputs, outputs));
            }
        }
    }

    Ok((io, lookup_table_ch, receivers))
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

        // Contains the node uids as well as its ports uids: node_uid, intputs, outputs
        let mut nodes_ports_uids: HashMap<
            NodeId,
            (u32, HashMap<PortId, u32>, HashMap<PortId, u32>),
        > = HashMap::new();

        let mut node_indexes: HashMap<u32, NodeIndex<u32>> = HashMap::new();
        let mut graph = StableGraph::<u32, (u32, u32)>::new();

        node_ids.append(&mut dataflow.sources.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.operators.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.sinks.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.connectors.keys().cloned().collect::<Vec<_>>());

        // Populating hashmap and graph with uids
        for (_, n) in &dataflow.sources {
            let uid = n.uid;
            let inputs = HashMap::new();
            let mut outputs = HashMap::new();

            outputs.insert(n.output.port_id.clone(), n.output.uid);

            nodes_ports_uids.insert(n.id.clone(), (uid, inputs, outputs));

            node_indexes.insert(uid, graph.add_node(uid));
        }

        for (_, n) in &dataflow.sinks {
            let uid = n.uid;
            let mut inputs = HashMap::new();
            let outputs = HashMap::new();

            inputs.insert(n.input.port_id.clone(), n.input.uid);

            nodes_ports_uids.insert(n.id.clone(), (uid, inputs, outputs));
            node_indexes.insert(uid, graph.add_node(uid));
        }

        for (_, n) in &dataflow.operators {
            let uid = n.uid;
            let mut inputs = HashMap::new();
            let mut outputs = HashMap::new();

            for (_, i) in &n.inputs {
                inputs.insert(i.port_id.clone(), i.uid);
            }
            for (_, o) in &n.outputs {
                outputs.insert(o.port_id.clone(), o.uid);
            }

            nodes_ports_uids.insert(n.id.clone(), (uid, inputs, outputs));
            node_indexes.insert(uid, graph.add_node(uid));
        }

        for (_, n) in &dataflow.connectors {
            let uid = n.uid;
            let mut inputs = HashMap::new();
            let mut outputs = HashMap::new();

            match n.kind {
                ZFConnectorKind::Receiver => {
                    inputs.insert(n.port.port_id.clone(), n.port.uid);
                }
                ZFConnectorKind::Sender => {
                    outputs.insert(n.port.port_id.clone(), n.port.uid);
                }
            }
            nodes_ports_uids.insert(n.id.clone(), (uid, inputs, outputs));

            node_indexes.insert(uid, graph.add_node(uid));
        }
        //

        let (mut links, lookup_table, receivers) = create_links(
            &node_ids,
            &dataflow.links,
            &nodes_ports_uids,
            &mut graph,
            &node_indexes,
        )?;

        let dot = Dot::new(&graph);
        log::error!("{dot:?}");

        let context = InstanceContext {
            flow_id: dataflow.flow_id,
            instance_id: dataflow.uuid,
            runtime: dataflow.context,
        };

        // The links were created, we can generate the Runners.
        let mut runners: HashMap<NodeId, Box<dyn Runner>> = HashMap::with_capacity(node_ids.len());

        for (id, source) in dataflow.sources.into_iter() {
            let (_, outputs) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Source < {} > were not created.",
                    &source.id
                ))
            })?;
            runners.insert(
                id,
                Box::new(SourceRunner::new(context.clone(), source, outputs)),
            );
        }

        for (id, operator) in dataflow.operators.into_iter() {
            let (inputs, outputs) = links.remove(&operator.id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Operator < {} > were not created.",
                    &operator.id
                ))
            })?;
            runners.insert(
                id,
                Box::new(OperatorRunner::new(
                    context.clone(),
                    operator,
                    inputs,
                    outputs,
                )),
            );
        }

        for (id, sink) in dataflow.sinks.into_iter() {
            let (inputs, _) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!("Links for Sink < {} > were not created.", &sink.id))
            })?;
            runners.insert(id, Box::new(SinkRunner::new(context.clone(), sink, inputs)));
        }

        for (id, connector) in dataflow.connectors.into_iter() {
            let (inputs, outputs) = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Connector < {} > were not created.",
                    &connector.id
                ))
            })?;
            match connector.kind {
                ZFConnectorKind::Sender => {
                    runners.insert(
                        id,
                        Box::new(ZenohSender::try_new(context.clone(), connector, inputs)?),
                    );
                }
                ZFConnectorKind::Receiver => {
                    runners.insert(
                        id,
                        Box::new(ZenohReceiver::try_new(context.clone(), connector, outputs)?),
                    );
                }
            }
        }

        let c_context = context.clone();
        //spawn forwarding task
        let fwd_loop = async move {
            //fake timestamping
            let buf = [0x00, 0x00];
            let id = ID::try_from(&buf[..1]).unwrap();
            let ts = Timestamp::new(uhlc::NTP64(0), id);

            let mut links = Vec::with_capacity(receivers.len());

            for r in receivers.values() {
                links.push(r.recv());
            }

            loop {
                match future::select_all(links).await {
                    (Ok((uids, data)), _index, remaining) => {
                        let data = Arc::try_unwrap(data).unwrap();
                        match lookup_table.get(&uids) {
                            Some(outs) => {
                                let msg = Arc::new(Message::from_serdedata(data, ts));
                                for s in outs {
                                    s.send(msg.clone()).await.unwrap();
                                }
                            }
                            None => log::error!("The link {uids:?} is not connected downstream!!"),
                        };
                        links = remaining;
                        links.push(receivers.get(&uids).unwrap().recv());
                    }
                    (Err(e), _index, remaining) => {
                        let err_msg = format!(
                            "[Instance FWD Loop: {}] Link returned an error: {:?}",
                            c_context.instance_id, e
                        );
                        log::error!("{err_msg}");
                        links = remaining;
                    }
                }
            }
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let join_handle = async_std::task::spawn(Abortable::new(fwd_loop, abort_registration));

        Ok(Self {
            context,
            runners,
            graph,
            abort_handle,
            join_handle,
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
    pub fn get_instance_context(&self) -> InstanceContext {
        self.context.clone()
    }

    /// Returns the `NodeId` for all the sources in this instance.
    pub fn get_sources(&self) -> Vec<NodeId> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Source))
            .map(|runner| runner.get_id())
            .collect()
    }

    /// Returns the `NodeId` for all the sinks in this instance.
    pub fn get_sinks(&self) -> Vec<NodeId> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Sink))
            .map(|runner| runner.get_id())
            .collect()
    }

    /// Returns the `NodeId` for all the sinks in this instance.
    pub fn get_operators(&self) -> Vec<NodeId> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Operator))
            .map(|runner| runner.get_id())
            .collect()
    }

    /// Returns the `NodeId` for all the connectors in this instance.
    pub fn get_connectors(&self) -> Vec<NodeId> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Connector))
            .map(|runner| runner.get_id())
            .collect()
    }

    /// Returns the `NodeId` for all the nodes in this instance.
    pub fn get_nodes(&self) -> Vec<NodeId> {
        self.runners
            .values()
            .map(|runner| runner.get_id())
            .collect()
    }

    /// Starts all the sources in this instance.
    ///
    ///
    /// **Note:** Not implemented.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  sources already started.
    pub async fn start_sources(&mut self) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }

    /// Starts all the nodes in this instance.
    ///
    /// **Note:** Not implemented.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  nodes already started.
    pub async fn start_nodes(&mut self) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }

    /// Stops all the sources in this instance.
    ///
    /// **Note:** Not implemented.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  sources already stopped.
    pub async fn stop_sources(&mut self) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }

    /// Stops all the sources in this instance.
    ///
    /// **Note:** Not implemented.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  nodes already stopped.
    pub async fn stop_nodes(&mut self) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }

    /// Checks if the given node is running.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn is_node_running(&self, node_id: &NodeId) -> ZFResult<bool> {
        if let Some(runner) = self.runners.get(node_id) {
            return Ok(runner.is_running().await);
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    /// Starts the given node.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn start_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            return runner.start().await;
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    /// Stops the given node.
    ///
    /// # Errors
    /// If fails if the node is not found or it is not running.
    pub async fn stop_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            runner.stop().await?;
            return Ok(());
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }

    /// Finalized the given node.
    /// Finalizing a node means cleaning up its state.
    ///
    /// # Errors
    /// If fails if the node is not found.
    pub async fn clean_node(&mut self, node_id: &NodeId) -> ZFResult<()> {
        let runner = self
            .runners
            .get(node_id)
            .ok_or_else(|| ZFError::NodeNotFound(node_id.clone()))?;
        runner.clean().await
    }

    // /// Starts the recording for the given source.
    // ///
    // /// It returns the key expression where the recording is stored.
    // ///
    // /// # Errors
    // /// If fails if the node is not found.
    // pub async fn start_recording(&self, node_id: &NodeId) -> ZFResult<String> {
    //     if let Some(runner) = self.runners.get(node_id) {
    //         return runner.start_recording().await;
    //     }

    //     Err(ZFError::NodeNotFound(node_id.clone()))
    // }

    // /// Stops the recording for the given source.
    // ///
    // /// It returns the key expression where the recording is stored.
    // ///
    // /// # Errors
    // /// If fails if the node is not found.
    // pub async fn stop_recording(&self, node_id: &NodeId) -> ZFResult<String> {
    //     if let Some(runner) = self.runners.get(node_id) {
    //         return runner.stop_recording().await;
    //     }

    //     Err(ZFError::NodeNotFound(node_id.clone()))
    // }

    // /// Assumes the source is already stopped before calling the start replay!
    // /// This method is called by the runtime, that always check that the node
    // /// is not running prior to call this function.
    // /// If someone is using directly the DataflowInstance need to stop and check
    // /// if the node is running before calling this function.
    // ///
    // /// # Errors
    // /// It fails if:
    // /// - the source is not stopped
    // /// - the node is not a source
    // /// - the key expression is not point to a recording
    // pub async fn start_replay(&mut self, source_id: &NodeId, resource: String) -> ZFResult<NodeId> {
    //     let runner = self
    //         .runners
    //         .get(source_id)
    //         .ok_or_else(|| ZFError::NodeNotFound(source_id.clone()))?;

    //     let mut outputs: Vec<(PortId, PortType)> = runner
    //         .get_outputs()
    //         .iter()
    //         .map(|(k, v)| (k.clone(), v.clone()))
    //         .collect();

    //     let (output_id, _output_type) = outputs
    //         .pop()
    //         .ok_or_else(|| ZFError::NodeNotFound(source_id.clone()))?;

    //     let replay_id: NodeId = format!(
    //         "replay-{}-{}-{}-{}",
    //         self.context.flow_id, self.context.instance_id, source_id, output_id
    //     )
    //     .into();

    //     let output_links = runner
    //         .get_outputs_links()
    //         .await
    //         .remove(&output_id)
    //         .ok_or_else(|| ZFError::PortNotFound((source_id.clone(), output_id.clone())))?;

    //     let replay_node = ZenohReplay::try_new(
    //         replay_id.clone(),
    //         self.context.clone(),
    //         source_id.clone(),
    //         output_id,
    //         _output_type,
    //         output_links,
    //         resource,
    //     )?;

    //     self.runners
    //         .insert(replay_id.clone(), Box::new(replay_node));
    //     Ok(replay_id)
    // }

    // /// Stops the recording for the given source.
    // ///
    // /// # Errors
    // /// If fails if the node is not found.
    // pub async fn stop_replay(&mut self, replay_id: &NodeId) -> ZFResult<()> {
    //     self.stop_node(replay_id).await?;
    //     self.runners.remove(replay_id);
    //     Ok(())
    // }
}

impl Drop for DataflowInstance {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}
