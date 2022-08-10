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

use std::collections::HashMap;
use std::time::Instant;

use crate::async_std::sync::Arc;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::SinkLoaded;
use crate::runtime::InstanceContext;
use crate::types::ZFResult;
use crate::{Configuration, Context, Input, NodeId, PortId, Sink, ZFError};
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// The `SinkRunner` is the component in charge of executing the sink.
/// It contains all the runtime information for the sink, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the sink/lib, otherwise we
/// will have a SIGSEV.
pub struct SinkRunner {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) inputs: HashMap<PortId, Input>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) callbacks_handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) callbacks_abort_handle: Option<AbortHandle>,
}

impl SinkRunner {
    /// Tries to create a new `SinkRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`SinkLoaded`](`SinkLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the input is not connected.
    pub fn new(
        instance_context: Arc<InstanceContext>,
        sink: SinkLoaded,
        inputs: HashMap<PortId, Input>,
    ) -> Self {
        Self {
            id: sink.id,
            configuration: sink.configuration,
            context: Context::new(instance_context),
            inputs,
            sink: sink.sink,
            _library: sink.library,
            handle: None,
            abort_handle: None,
            callbacks_handle: None,
            callbacks_abort_handle: None,
        }
    }
}

#[async_trait]
impl Runner for SinkRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Sink
    }

    async fn is_running(&self) -> bool {
        self.handle.is_some() || self.callbacks_handle.is_some()
    }

    async fn stop(&mut self) -> ZFResult<()> {
        // Stop is idempotent, if the node was already stopped,
        // do nothing and return Ok(())

        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort()
        }

        if let Some(handle) = self.handle.take() {
            log::trace!("Sink handler finished with {:?}", handle.await);
        }

        Ok(())
    }

    async fn start(&mut self) -> ZFResult<()> {
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Sink: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        log::trace!("[Sink: {}] Starting", self.id);

        let iteration = self
            .sink
            .setup(&mut self.context, &self.configuration, self.inputs.clone())
            .await?;

        /* Callbacks */
        let cb_receivers = std::mem::take(&mut self.context.callback_receivers);
        if !cb_receivers.is_empty() {
            let c_id = Arc::clone(&self.id);
            let callbacks_receivers_loop = async move {
                let mut cbs: Vec<_> = cb_receivers
                    .iter()
                    .map(|callback| Box::pin(callback.run()))
                    .collect();

                loop {
                    let (res, _, remainings) = futures::future::select_all(cbs).await;
                    cbs = remainings;
                    match res {
                        Err(e) => {
                            log::error!("[Source: {c_id}] {:?}", e);
                            return e;
                        }
                        Ok(index) => {
                            cbs.push(Box::pin(cb_receivers[index].run()));
                        }
                    }

                    async_std::task::yield_now().await;
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle = async_std::task::spawn(Abortable::new(
                callbacks_receivers_loop,
                cb_abort_registration,
            ));
            self.callbacks_handle = Some(cb_handle);
            self.callbacks_abort_handle = Some(cb_abort_handle);
        }

        /* Streams */
        if let Some(iteration) = iteration {
            let c_id = self.id.clone();
            let run_loop = async move {
                let mut instant: Instant;
                loop {
                    instant = Instant::now();
                    if let Err(e) = iteration.call().await {
                        log::error!("[Sink: {c_id}] {:?}", e);
                        return e;
                    }

                    log::trace!(
                        "[Sink: {c_id}] iteration took: {}ms",
                        instant.elapsed().as_millis()
                    );

                    async_std::task::yield_now().await;
                }
            };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

            self.handle = Some(handle);
            self.abort_handle = Some(abort_handle);
        }

        Ok(())
    }
}
