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

use crate::async_std::sync::Arc;
use crate::runtime::dataflow::node::SinkLoaded;
use crate::runtime::Context;
use crate::{Configuration, Inputs, NodeId};

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

use super::{Ready, Running};

// /// The `SinkRunner` is the component in charge of executing the sink.
// /// It contains all the runtime information for the sink, the graph instance.
// ///
// /// Do not reorder the fields in this struct.
// /// Rust drops fields in a struct in the same order they are declared.
// /// Ref: <https://doc.rust-lang.org/reference/destructors.html>
// /// We need the state to be dropped before the sink/lib, otherwise we
// /// will have a SIGSEV.
// #[derive(Clone)]
// pub struct SinkRunner {
//     pub(crate) id: NodeId,
//     // pub(crate) context: InstanceContext,
//     pub(crate) input: PortRecord,
//     pub(crate) inputs: Inputs,
//     pub(crate) context: Context,
//     // pub(crate) input: PortDescriptor,
//     // pub(crate) link: Arc<Mutex<Option<LinkReceiver>>>,
//     pub(crate) _end_to_end_deadlines: Vec<E2EDeadlineRecord>, //FIXME
//     pub(crate) is_running: Arc<Mutex<bool>>,
//     // pub(crate) state: Arc<Mutex<State>>,
//     pub(crate) sink: Arc<dyn Sink>,
//     pub(crate) _library: Option<Arc<Library>>,
// }

// impl SinkRunner {
//     /// Tries to create a new `SinkRunner` using the given
//     /// [`InstanceContext`](`InstanceContext`), [`SinkLoaded`](`SinkLoaded`)
//     /// and [`OperatorIO`](`OperatorIO`).
//     ///
//     /// # Errors
//     /// If fails if the input is not connected.
//     pub fn try_new(context: Context, sink: SinkLoaded, inputs: Inputs) -> ZFResult<Self> {
//         let port_id = sink.input.port_id.clone();
//         // let link = inputs.remove(&port_id).ok_or_else(|| {
//         //     ZFError::MissingOutput(format!(
//         //         "Missing link for port < {} > for Sink: < {} >.",
//         //         &port_id, &sink.id
//         //     ))
//         // })?;

//         Ok(Self {
//             id: sink.id,
//             context,
//             inputs,
//             // link: Arc::new(Mutex::new(Some(link))),
//             _end_to_end_deadlines: vec![],
//             is_running: Arc::new(Mutex::new(false)),
//             // state: sink.state,
//             sink: sink.sink,
//             _library: sink.library,
//             input: sink.input,
//         })
//     }

//     /// Starts the sink.
//     async fn start(&self) {
//         *self.is_running.lock().await = true;
//     }

//     /// A single iteration of the run loop.
//     ///
//     /// # Errors
//     /// An error variant is returned in case of:
//     /// -  user returns an error
//     /// - link recv fails
//     ///
//     async fn iteration(&self, context: Context) -> ZFResult<Context> {
//         // // Guards are taken at the beginning of each iteration to allow interleaving.
//         // if let Some(link) = &*self.link.lock().await {
//         //     let mut state = self.state.lock().await;

//         //     let (port_id, message) = link.recv().await?;
//         //     let input = match message.as_ref() {
//         //         Message::Data(data_message) => {
//         //             if let Err(error) = self
//         //                 .context
//         //                 .runtime
//         //                 .hlc
//         //                 .update_with_timestamp(&data_message.timestamp)
//         //             {
//         //                 log::error!(
//         //                     "[Sink: {}][HLC] Could not update HLC with timestamp {:?}: {:?}",
//         //                     self.id,
//         //                     data_message.timestamp,
//         //                     error
//         //                 );
//         //             }
//         //             let now = self.context.runtime.hlc.new_timestamp();
//         //             let mut input = data_message.clone();

//         //             data_message
//         //                 .end_to_end_deadlines
//         //                 .iter()
//         //                 .for_each(|e2e_deadline| {
//         //                     if let Some(miss) = e2e_deadline.check(&self.id, &port_id, &now) {
//         //                         input.missed_end_to_end_deadlines.push(miss);
//         //                     }
//         //                 });

//         //             input
//         //         }

//         //         Message::Control(_) => return Err(ZFError::Unimplemented),
//         //     };

//         //     self.sink.run(&mut context, &mut state, input).await?;
//         // }
//         Ok(context)
//     }
// }

// #[async_trait]
// impl Runnable for SinkRunner {
//     fn get_id(&self) -> NodeId {
//         self.id.clone()
//     }

//     fn get_kind(&self) -> RunnerKind {
//         RunnerKind::Sink
//     }

//     // async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
//     //     (*self.link.lock().await) = Some(input);
//     //     Ok(())
//     // }

//     // async fn add_output(&self, _output: LinkSender) -> ZFResult<()> {
//     //     Err(ZFError::SinkDoNotHaveOutputs)
//     // }

//     // async fn clean(self) -> ZFResult<()> {
//     //     self.sink.finalize().await
//     // }

//     // fn get_inputs(&self) -> HashMap<PortId, PortType> {
//     //     let mut inputs = HashMap::with_capacity(1);
//     //     inputs.insert(self.input.port_id.clone(), self.input.port_type.clone());
//     //     inputs
//     // }

//     // fn get_outputs(&self) -> HashMap<PortId, PortType> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
//     //     let mut link_guard = self.link.lock().await;
//     //     if let Some(link) = &*link_guard {
//     //         let mut inputs = HashMap::with_capacity(1);
//     //         inputs.insert(self.input.port_id.clone(), link.clone());
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
//         *self.is_running.lock().await = false;
//     }

//     async fn run(&self) -> ZFResult<()> {
//         self.start().await;

//         // let mut context = Context::default();
//         // Looping on iteration, each iteration is a single
//         // run of the source, as a run can fail in case of error it
//         // stops and returns the error to the caller (the RunnerManager)

//         // loop {
//         //     match self.iteration(context).await {
//         //         Ok(ctx) => {
//         //             // log::trace!(
//         //             //     "[Sink: {}] iteration ok with new context {:?}",
//         //             //     self.id,
//         //             //     ctx
//         //             // );
//         //             context = ctx;
//         //             // As async_std scheduler is run to completion,
//         //             // if the iteration is always ready there is a possibility
//         //             // that other tasks are not scheduled (e.g. the stopping
//         //             // task), therefore after the iteration we give back
//         //             // the control to the scheduler, if no other tasks are
//         //             // ready, then this one is scheduled again.
//         //             async_std::task::yield_now().await;
//         //             continue;
//         //         }
//         //         Err(e) => {
//         //             log::error!("[Sink: {}] iteration failed with error: {}", self.id, e);
//         //             self.stop().await;
//         //             break Err(e);
//         //         }
//         //     }
//         // }

//         Ok(())
//     }
// }

// // #[cfg(test)]
// // #[path = "./tests/sink_e2e_deadline_tests.rs"]
// // mod e2e_deadline_tests;

pub enum SinkRunner {
    Ready(Sink<Ready>),
    Running(Sink<Running>),
}

pub struct Sink<State> {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) inputs: Inputs,
    pub(crate) implementation: Arc<dyn crate::traits::Sink>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) state: State,
}

impl<T> Sink<T> {
    pub fn get_id(&self) -> &NodeId {
        &self.id
    }
}

impl Sink<Ready> {
    pub fn new(context: Context, sink: SinkLoaded, inputs: Inputs) -> Self {
        Self {
            id: sink.id,
            context,
            configuration: sink.configuration,
            inputs,
            implementation: sink.sink,
            _library: sink.library,
            state: Ready,
        }
    }

    pub fn start(self) -> Sink<Running> {
        let Self {
            id,
            mut context,
            configuration,
            inputs,
            implementation,
            _library,
            state: _,
        } = self;

        let c_inputs = inputs.clone();
        let c_id = id.clone();

        let iteration = async_std::task::block_on(async {
            implementation
                .setup(&mut context, &configuration, c_inputs)
                .await
        });

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async {
                loop {
                    if let Err(e) = iteration.call().await {
                        log::error!("[Sink: (id) {c_id}] {:?}", e);
                        return e;
                    }

                    async_std::task::yield_now().await;
                }
            })
        });

        Sink::<Running> {
            id,
            context,
            configuration,
            inputs,
            implementation,
            _library,
            state: Running { handle },
        }
    }
}

impl Sink<Running> {
    pub fn stop(self) -> Sink<Ready> {
        let Self {
            id,
            context,
            configuration,
            inputs,
            implementation,
            _library,
            state,
        } = self;

        async_std::task::block_on(async {
            state.handle.cancel().await;

            if let Err(e) = implementation.finalize().await {
                log::error!("[Sink: (id) {id}] {:?}", e);
            }
        });

        Sink::<Ready> {
            id,
            context,
            configuration,
            inputs,
            implementation,
            _library,
            state: Ready,
        }
    }
}
