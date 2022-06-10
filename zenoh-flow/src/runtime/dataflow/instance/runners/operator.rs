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

use crate::async_std::sync::{Arc, Mutex};
use crate::model::deadline::E2EDeadlineRecord;
use crate::model::loops::LoopDescriptor;
use crate::model::node::OperatorRecord;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::{
    Context, DataMessage, InputToken, NodeId, Operator, PortId, PortType, ZFError, ZFResult,
};
use async_trait::async_trait;
use futures::Future;
use std::collections::HashMap;
use std::time::Duration;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A struct that wraps the inputs and outputs
/// of a node.
#[derive(Default)]
pub struct OperatorIO {
    inputs: HashMap<PortId, LinkReceiver>,
    outputs: HashMap<PortId, Vec<LinkSender>>,
}

/// Future of the `Receiver`
type LinkRecvFut<'a> = std::pin::Pin<
    Box<dyn Future<Output = Result<(Arc<str>, Arc<Message>), ZFError>> + Send + Sync + 'a>,
>;

impl OperatorIO {
    fn poll_input(&self, node_id: &NodeId, port_id: &PortId) -> ZFResult<LinkRecvFut> {
        let rx = self.inputs.get(port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "[Operator: {}] Link < {} > no longer exists.",
                node_id, port_id
            ))
        })?;
        Ok(rx.recv())
    }
}

/// Type of Inputs
pub type InputsLink = HashMap<PortId, LinkReceiver>;
/// Type of Outputs
pub type OutputsLinks = HashMap<PortId, Vec<LinkSender>>;

impl OperatorIO {
    /// Creates the `OperatorIO` from an [`OperatorRecord`](`OperatorRecord`)
    pub fn new(record: &OperatorRecord) -> Self {
        Self {
            inputs: HashMap::with_capacity(record.inputs.len()),
            outputs: HashMap::with_capacity(record.outputs.len()),
        }
    }

    /// Tries to add the given `LinkReceiver`
    ///
    /// # Errors
    /// It fails if the `PortId` is duplicated.
    pub fn try_add_input(&mut self, rx: LinkReceiver) -> ZFResult<()> {
        if self.inputs.contains_key(&rx.id()) {
            return Err(ZFError::DuplicatedPort((rx.id(), rx.id())));
        }

        self.inputs.insert(rx.id(), rx);

        Ok(())
    }

    /// Adds the given `LinkSender`
    pub fn add_output(&mut self, tx: LinkSender) {
        if let Some(vec_senders) = self.outputs.get_mut(&tx.id()) {
            vec_senders.push(tx);
        } else {
            self.outputs.insert(tx.id(), vec![tx]);
        }
    }

    /// Destroys and returns a tuple with the internal inputs and outputs.
    pub fn take(self) -> (InputsLink, OutputsLinks) {
        (self.inputs, self.outputs)
    }

    /// Returns a copy of the inputs.
    pub fn get_inputs(&self) -> InputsLink {
        self.inputs.clone()
    }

    /// Returns a copy of the outputs.
    pub fn get_outputs(&self) -> OutputsLinks {
        self.outputs.clone()
    }
}

/// The `OperatorRunner` is the component in charge of executing the operator.
/// It contains all the runtime information for the operator, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the operator/lib, otherwise we
/// will have a SIGSEV.
#[derive(Clone)]
pub struct OperatorRunner {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) io: Arc<Mutex<OperatorIO>>,
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) local_deadline: Option<Duration>,
    pub(crate) end_to_end_deadlines: Vec<E2EDeadlineRecord>,
    pub(crate) is_running: Arc<Mutex<bool>>,
    // Ciclo is the italian word for "loop" — we cannot use "loop" as it’s a reserved keyword.
    pub(crate) ciclo: Option<LoopDescriptor>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) _library: Option<Arc<Library>>,
}

impl OperatorRunner {
    /// Tries to create a new `OperatorRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`OperatorLoaded`](`OperatorLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the output is not connected.
    pub fn try_new(
        context: InstanceContext,
        operator: OperatorLoaded,
        operator_io: OperatorIO,
    ) -> ZFResult<Self> {
        // TODO Check that all ports are used.
        Ok(Self {
            id: operator.id,
            context,
            io: Arc::new(Mutex::new(operator_io)),
            inputs: operator.inputs,
            outputs: operator.outputs,
            is_running: Arc::new(Mutex::new(false)),
            operator: operator.operator,
            _library: operator.library,
            local_deadline: operator.local_deadline,
            end_to_end_deadlines: operator.end_to_end_deadlines,
            ciclo: operator.ciclo,
        })
    }

    /// Starts the operator.
    async fn start(&self) {
        *self.is_running.lock().await = true;
    }

    /// A single iteration of the run loop.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  user returns an error
    /// - link recv fails
    /// - link send fails
    ///
    async fn iteration(
        &self,
        context: Context,
        tokens: HashMap<PortId, InputToken>,
        data: HashMap<PortId, DataMessage>,
    ) -> ZFResult<(
        Context,
        HashMap<PortId, InputToken>,
        HashMap<PortId, DataMessage>,
    )> {
        Ok((context, tokens, data))
    }
}

#[async_trait]
impl Runner for OperatorRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Operator
    }

    async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
        let mut guard = self.io.lock().await;
        let key = input.id();
        guard.inputs.insert(key, input);
        Ok(())
    }

    async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
        let mut guard = self.io.lock().await;
        let key = output.id();
        if let Some(links) = guard.outputs.get_mut(key.as_ref()) {
            links.push(output);
        } else {
            guard.outputs.insert(key, vec![output]);
        }
        Ok(())
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        self.inputs.clone()
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        self.outputs.clone()
    }

    async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
        self.io.lock().await.get_outputs()
    }

    async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
        let inputs = HashMap::new();
        let mut io_guard = self.io.lock().await;
        let current_inputs = io_guard.get_inputs();
        io_guard.inputs = inputs;
        current_inputs
    }

    async fn start_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unsupported)
    }

    async fn stop_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unsupported)
    }

    async fn is_recording(&self) -> bool {
        false
    }

    async fn stop(&self) {
        *self.is_running.lock().await = false;
    }

    async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    // async fn clean(&self) -> ZFResult<()> {
    //     let mut state = self.state.lock().await;
    //     self.operator.finalize(&mut state)
    // }

    async fn run(&self) -> ZFResult<()> {
        self.start().await;

        let mut context = Context::default();
        let mut tokens: HashMap<PortId, InputToken> = self
            .inputs
            .keys()
            .map(|input_id| (input_id.clone(), InputToken::Pending))
            .collect();
        let mut data: HashMap<PortId, DataMessage> = HashMap::with_capacity(tokens.len());

        Ok(())

        // Looping on iteration, each iteration is a single
        // run of the source, as a run can fail in case of error it
        // stops and returns the error to the caller (the RunnerManager)
        // loop {
        //     match self.iteration(context, tokens, data).await {
        //         Ok((ctx, tkn, d)) => {
        //             log::trace!(
        //                 "[Operator: {}] iteration ok with new context {:?}",
        //                 self.id,
        //                 ctx
        //             );
        //             context = ctx;
        //             tokens = tkn;
        //             data = d;
        //             // As async_std scheduler is run to completion,
        //             // if the iteration is always ready there is a possibility
        //             // that other tasks are not scheduled (e.g. the stopping
        //             // task), therefore after the iteration we give back
        //             // the control to the scheduler, if no other tasks are
        //             // ready, then this one is scheduled again.
        //             async_std::task::yield_now().await;
        //             continue;
        //         }
        //         Err(e) => {
        //             log::error!("[Operator: {}] iteration failed with error: {}", self.id, e);
        //             self.stop().await;
        //             break Err(e);
        //         }
        //     }
        // }
    }
}

// #[cfg(test)]
// #[path = "./tests/operator_test.rs"]
// mod tests;

// #[cfg(test)]
// #[path = "./tests/operator_e2e_deadline_tests.rs"]
// mod e2e_deadline_tests;
