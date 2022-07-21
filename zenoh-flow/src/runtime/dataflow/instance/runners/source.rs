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
use crate::runtime::dataflow::node::SourceLoaded;
use crate::runtime::Context;
use crate::{Configuration, NodeId, Outputs};

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

use super::{Ready, Running};

// /// The `SourceRunner` is the component in charge of executing the source.
// ///
// /// It contains all the runtime information for the source, the graph instance.
// ///
// /// Do not reorder the fields in this struct. Rust drops fields in a struct in the same order they
// /// are declared.
// /// Ref: <https://doc.rust-lang.org/reference/destructors.html>
// ///
// /// We need the state to be dropped before the source/lib, otherwise we will have a SIGSEV.
// #[derive(Clone)]
// pub struct SourceRunner {
//     pub(crate) id: NodeId,
//     pub(crate) context: Context,
//     pub(crate) configuration: Option<Configuration>,
//     pub(crate) _period: Option<Duration>,
//     pub(crate) output: PortRecord,
//     // pub(crate) links: Arc<Mutex<Vec<LinkSender>>>,
//     // pub(crate) output: PortDescriptor,
//     // pub(crate) links: Arc<Mutex<Vec<LinkSender>>>,
//     pub(crate) outputs: Outputs,
//     pub(crate) base_resource_name: String,
//     pub(crate) current_recording_resource_id: Arc<Mutex<Option<ExprId>>>,
//     pub(crate) current_recording_resource: Arc<Mutex<Option<String>>>,
//     pub(crate) is_recording: Arc<Mutex<bool>>,
//     pub(crate) is_running: Arc<Mutex<bool>>,
//     pub(crate) source: Arc<dyn Source>,
//     pub(crate) _library: Option<Arc<Library>>,
//     // pub(crate) state_machine: RunnerStateMachine,
// }

// impl SourceRunner {
//     /// Tries to create a new `SourceRunner` using the given [`InstanceContext`](`InstanceContext`),
//     /// [`SourceLoaded`](`SourceLoaded`).
//     ///
//     /// # Errors
//     ///
//     /// This method will fail if the output is not connected.
//     pub fn try_new(
//         context: Context,
//         source: SourceLoaded,
//         io: (Inputs, Outputs),
//     ) -> ZFResult<Self> {
//         let port_id = source.output.port_id.clone();
//         let (_, outputs) = io;

//         let base_resource_name = format!(
//             "/zf/record/{}/{}/{}/{}",
//             &context.flow_id, &context.instance_id, source.id, port_id
//         );

//         // Declaring the recording resource to reduce network overhead.
//         let base_resource_id = context
//             .runtime
//             .session
//             .declare_expr(&base_resource_name)
//             .wait()?;

//         Ok(Self {
//             id: source.id,
//             context,
//             _period: source.period,
//             source: source.source,
//             _library: source.library,
//             base_resource_name,
//             current_recording_resource_id: Arc::new(Mutex::new(None)),
//             is_recording: Arc::new(Mutex::new(false)),
//             is_running: Arc::new(Mutex::new(false)),
//             current_recording_resource: Arc::new(Mutex::new(None)),
//             // state_machine: RunnerStateMachine::Uninit,
//             configuration: source.configuration,
//             outputs,
//             output: source.output,
//         })
//     }

//     /// Records the given `message`.
//     ///
//     /// # Errors
//     /// An error variant is returned in case of:
//     /// - unable to put on zenoh
//     /// - serialization fails
//     async fn record(&self, message: Arc<Message>) -> ZFResult<()> {
//         log::trace!("ZenohLogger IN <= {:?} ", message);
//         let recording = self.is_recording.lock().await;

//         if !(*recording) {
//             log::trace!("ZenohLogger Dropping!");
//             return Ok(());
//         }

//         let resource_name_guard = self.current_recording_resource.lock().await;
//         let resource_id_guard = self.current_recording_resource_id.lock().await;
//         let resource_id = resource_id_guard
//             .as_ref()
//             .ok_or(ZFError::Unimplemented)?
//             .clone();

//         let serialized = message.serialize_bincode()?;
//         log::trace!(
//             "ZenohLogger - {:?} => {:?} ",
//             resource_name_guard,
//             serialized
//         );

//         self.context
//             .runtime
//             .session
//             .put(&resource_id, serialized)
//             .congestion_control(CongestionControl::Block)
//             .await?;

//         Ok(())
//     }

//     async fn setup(&mut self) -> ZFResult<()> {
//         // let outputs = Outputs {
//         //     hmap: HashMap::from(("fixme".into(), self.output)),
//         // };

//         // let async_iteration = self
//         //     .source
//         //     .setup(&mut self.context, &self.configuration, outputs)
//         //     .await;

//         Ok(())
//     }

//     // /// A single iteration of the run loop.
//     // ///
//     // /// # Errors
//     // /// An error variant is returned in case of:
//     // /// - user returns an error
//     // /// - record fails
//     // /// - link send fails
//     // async fn iteration(&self, mut context: Context) -> ZFResult<Context> {
//     //     let links = self.links.lock().await;

//     //     // Running
//     //     let output = self.source.run(&mut context, &mut state).await?;

//     //     let timestamp = self.context.runtime.hlc.new_timestamp();

//     //     let e2e_deadlines = self
//     //         .end_to_end_deadlines
//     //         .iter()
//     //         .map(|deadline| E2EDeadline::new(deadline.clone(), timestamp))
//     //         .collect();

//     //     // Send to Links
//     //     log::trace!("Sending on {:?} data: {:?}", self.output.port_id, output);

//     //     let zf_message = Arc::new(Message::from_serdedata(
//     //         output,
//     //         timestamp,
//     //         e2e_deadlines,
//     //         vec![],
//     //     ));
//     //     for link in links.iter() {
//     //         log::trace!("\tSending on: {:?}", link);
//     //         link.send(zf_message.clone()).await?;
//     //     }
//     //     self.record(zf_message).await?;
//     //     Ok(context)
//     // }

//     /// Starts the source.
//     async fn start(&self) {
//         *self.is_running.lock().await = true;
//     }
// }

// #[async_trait]
// impl Runnable for SourceRunner {
//     fn get_id(&self) -> NodeId {
//         self.id.clone()
//     }

//     fn get_kind(&self) -> RunnerKind {
//         RunnerKind::Source
//     }

//     // async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
//     //     (*self.links.lock().await).push(output);
//     //     Ok(())
//     // }

//     // async fn add_input(&self, _input: LinkReceiver) -> ZFResult<()> {
//     //     Err(ZFError::SourceDoNotHaveInputs)
//     // }

//     // fn get_outputs(&self) -> HashMap<PortId, PortType> {
//     //     let mut outputs = HashMap::with_capacity(1);
//     //     outputs.insert(self.output.port_id.clone(), self.output.port_type.clone());
//     //     outputs
//     // }

//     // fn get_inputs(&self) -> HashMap<PortId, PortType> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
//     //     let mut outputs = HashMap::with_capacity(1);
//     //     outputs.insert(self.output.port_id.clone(), self.links.lock().await.clone());
//     //     outputs
//     // }

//     // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
//     //     HashMap::with_capacity(0)
//     // }

//     // async fn start_recording(&self) -> ZFResult<String> {
//     //     let mut is_recording_guard = self.is_recording.lock().await;
//     //     if !(*is_recording_guard) {
//     //         let ts_recording_start = self.context.runtime.hlc.new_timestamp();
//     //         let resource_name = format!(
//     //             "{}/{}",
//     //             self.base_resource_name,
//     //             ts_recording_start.get_time()
//     //         );

//     //         let recording_id = self
//     //             .context
//     //             .runtime
//     //             .session
//     //             .declare_expr(&resource_name)
//     //             .await?;

//     //         *(self.current_recording_resource_id.lock().await) = Some(recording_id.clone());
//     //         *(self.current_recording_resource.lock().await) = Some(resource_name.clone());

//     //         let recording_metadata = RecordingMetadata {
//     //             timestamp: ts_recording_start,
//     //             port_id: self.output.port_id.clone(),
//     //             node_id: self.id.clone(),
//     //             flow_id: self.context.flow_id.clone(),
//     //             instance_id: self.context.instance_id,
//     //         };

//     //         let message = Message::Control(ControlMessage::RecordingStart(recording_metadata));
//     //         let serialized = message.serialize_bincode()?;
//     //         log::trace!(
//     //             "ZenohLogger - {} - Started recording at {:?}",
//     //             resource_name,
//     //             ts_recording_start
//     //         );
//     //         self.context
//     //             .runtime
//     //             .session
//     //             .put(&resource_name, serialized)
//     //             .await?;
//     //         *is_recording_guard = true;
//     //         return Ok(resource_name);
//     //     }
//     //     return Err(ZFError::AlreadyRecording);
//     // }

//     // async fn stop_recording(&self) -> ZFResult<String> {
//     //     let mut is_recording_guard = self.is_recording.lock().await;
//     //     if *is_recording_guard {
//     //         let mut resource_name_guard = self.current_recording_resource.lock().await;

//     //         let resource_name = resource_name_guard
//     //             .as_ref()
//     //             .ok_or(ZFError::Unimplemented)?
//     //             .clone();

//     //         let mut resource_id_guard = self.current_recording_resource_id.lock().await;
//     //         let resource_id = resource_id_guard
//     //             .as_ref()
//     //             .ok_or(ZFError::Unimplemented)?
//     //             .clone();

//     //         let ts_recording_stop = self.context.runtime.hlc.new_timestamp();
//     //         let message = Message::Control(ControlMessage::RecordingStop(ts_recording_stop));
//     //         let serialized = message.serialize_bincode()?;
//     //         log::debug!(
//     //             "ZenohLogger - {} - Stop recording at {:?}",
//     //             resource_name,
//     //             ts_recording_stop
//     //         );
//     //         self.context
//     //             .runtime
//     //             .session
//     //             .put(resource_id, serialized)
//     //             .await?;

//     //         *is_recording_guard = false;
//     //         *resource_name_guard = None;
//     //         *resource_id_guard = None;

//     //         self.context
//     //             .runtime
//     //             .session
//     //             .undeclare_expr(resource_id)
//     //             .await?;

//     //         return Ok(resource_name);
//     //     }
//     //     return Err(ZFError::NotRecording);
//     // }

//     // async fn is_recording(&self) -> bool {
//     //     *self.is_recording.lock().await
//     // }

//     async fn is_running(&self) -> bool {
//         *self.is_running.lock().await
//     }

//     async fn stop(&self) {
//         *self.is_running.lock().await = false;
//     }

//     // async fn clean(self) -> ZFResult<()> {
//     //     self.source.finalize().await
//     // }

//     async fn run(&self) -> ZFResult<()> {
//         self.start().await;

//         // let mut context = Context::default();
//         // Looping on iteration, each iteration is a single
//         // run of the source, as a run can fail in case of error it
//         // stops and returns the error to the caller (the RunnerManager)
//         // loop {
//         //     match self.iteration(context).await {
//         //         Ok(ctx) => {
//         //             log::trace!(
//         //                 "[Source: {}] iteration ok with new context {:?}",
//         //                 self.id,
//         //                 ctx
//         //             );
//         //             context = ctx;
//         //             if let Some(p) = self.period {
//         //                 async_std::task::sleep(p).await;
//         //             }
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
//         //             log::error!("[Source: {}] iteration failed with error: {}", self.id, e);
//         //             self.stop().await;
//         //             break Err(e);
//         //         }
//         //     }
//         // }
//         Ok(())
//     }
// }

// // #[cfg(test)]
// // #[path = "./tests/source_e2e_deadline_tests.rs"]
// // mod e2e_deadline_tests;

// // #[cfg(test)]
// // #[path = "./tests/source_periodic_test.rs"]
// // mod periodic_tests;

pub enum SourceRunner {
    Ready(Source<Ready>),
    Running(Source<Running>),
}

pub struct Source<State> {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) outputs: Outputs,
    // pub(crate) base_resource_name: String,
    // pub(crate) base_resource_id: u64,
    // pub(crate) current_recording_resource_id: Arc<Mutex<Option<ExprId>>>,
    // pub(crate) current_recording_resource: Arc<Mutex<Option<String>>>,
    pub(crate) implementation: Arc<dyn crate::traits::Source>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) state: State,
}

impl<T> Source<T> {
    pub fn get_id(&self) -> &NodeId {
        &self.id
    }
}

impl Source<Ready> {
    pub fn new(context: Context, source: SourceLoaded, io: Outputs) -> Self {
        // let base_resource_name = format!(
        //     "/zf/record/{}/{}/{}/{}",
        //     &context.flow_id, &context.instance_id, source.id, &source.output.port_id
        // );

        // // Declaring the recording resource to reduce network overhead.
        // let base_resource_id = context
        //     .runtime
        //     .session
        //     .declare_expr(&base_resource_name)
        //     .wait()?;

        Self {
            id: source.id,
            context,
            implementation: source.source,
            _library: source.library,
            // base_resource_name,
            // base_resource_id,
            // current_recording_resource_id: Arc::new(Mutex::new(None)),
            // current_recording_resource: Arc::new(Mutex::new(None)),
            configuration: source.configuration,
            outputs: io,
            state: Ready,
        }
    }

    pub fn start(self) -> Source<Running> {
        let Self {
            id,
            mut context,
            configuration,
            outputs,
            // base_resource_name,
            // base_resource_id,
            // current_recording_resource_id,
            // current_recording_resource,
            implementation,
            _library,
            state: _,
        } = self;

        let c_id = id.clone();

        // By blocking here instead of in the `spawn_blocking` we avoid having to copy/clone the
        // `context`, `configuration` and `implementation`.
        let iteration = async_std::task::block_on(async {
            // TODO Maybe move this call in the `try_new` above? If we do so, what’s the impact on the
            // call to `finalize` in the `stop` method below?
            implementation
                .setup(&mut context, &configuration, outputs.clone())
                .await
        });

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async {
                loop {
                    let res = iteration.call().await;

                    if let Err(e) = res {
                        log::error!("[Source: {c_id}] {:?}", e);
                        return e;
                    }

                    async_std::task::yield_now().await;
                }
            })
        });

        Source::<Running> {
            id,
            context,
            configuration,
            outputs,
            // base_resource_name,
            // base_resource_id,
            // current_recording_resource_id,
            // current_recording_resource,
            implementation,
            _library,
            state: Running { handle },
        }
    }
}

impl Source<Running> {
    pub fn stop(self) -> Source<Ready> {
        let Self {
            id,
            context,
            configuration,
            outputs,
            // base_resource_name,
            // base_resource_id,
            // current_recording_resource_id,
            // current_recording_resource,
            implementation,
            _library,
            state,
        } = self;

        async_std::task::block_on(async {
            state.handle.cancel().await;

            if let Err(e) = implementation.finalize().await {
                log::error!("[Source: {id}] {:?}", e);
            }
        });

        Source::<Ready> {
            id,
            context,
            configuration,
            outputs,
            // base_resource_name,
            // base_resource_id,
            // current_recording_resource_id,
            // current_recording_resource,
            implementation,
            _library,
            state: Ready,
        }
    }
}
