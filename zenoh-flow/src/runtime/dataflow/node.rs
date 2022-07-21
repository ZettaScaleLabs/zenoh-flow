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

use crate::model::link::PortRecord;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::{Configuration, NodeId, Operator, PortId, PortType, Sink, Source};
use async_std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A Source that was loaded, either dynamically or statically.
/// When a source is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the source.
pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) output: PortRecord,
    // pub(crate) output: PortDescriptor,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) period: Option<Duration>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SourceLoaded {
    pub fn new(record: SourceRecord, lib: Option<Arc<Library>>, source: Arc<dyn Source>) -> Self {
        Self {
            id: record.id,
            output: record.output,
            period: record.period.map(|dur_desc| dur_desc.to_duration()),
            source,
            library: lib,
            // end_to_end_deadlines: vec![],
            configuration: record.configuration,
        }
    }
}

/// An Operator that was loaded, either dynamically or statically.
/// When a operator is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the operator.
pub struct OperatorLoaded {
    pub(crate) id: NodeId,
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) configuration: Option<Configuration>,
    // pub(crate) local_deadline: Option<Duration>,
    // pub(crate) ciclo: Option<LoopDescriptor>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl OperatorLoaded {
    pub fn new(
        record: OperatorRecord,
        lib: Option<Arc<Library>>,
        operator: Arc<dyn Operator>,
    ) -> Self {
        let inputs: HashMap<PortId, PortType> = record
            .inputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        let outputs: HashMap<PortId, PortType> = record
            .outputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        Self {
            id: record.id,
            inputs,
            outputs,
            operator,
            library: lib,
            // end_to_end_deadlines: vec![],
            // ciclo: record.ciclo,
            configuration: record.configuration,
        }
    }
}

/// A Sink that was loaded, either dynamically or statically.
/// When a sink is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the sink.
pub struct SinkLoaded {
    pub(crate) id: NodeId,
    pub(crate) input: PortRecord,
    // pub(crate) input: PortDescriptor,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SinkLoaded {
    pub fn new(record: SinkRecord, lib: Option<Arc<Library>>, sink: Arc<dyn Sink>) -> Self {
        Self {
            id: record.id,
            input: record.input,
            sink,
            library: lib,
            // end_to_end_deadlines: vec![],
            configuration: record.configuration,
        }
    }
}
