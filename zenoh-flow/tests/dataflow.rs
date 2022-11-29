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

use async_trait::async_trait;
use flume::{bounded, Receiver};
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use zenoh::prelude::r#async::*;
use zenoh_flow::model::descriptor::{InputDescriptor, OutputDescriptor};
use zenoh_flow::model::record::{OperatorRecord, PortRecord, SinkRecord, SourceRecord};
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::traits::Factory;
use zenoh_flow::traits::ZFData;
use zenoh_flow::types::{Configuration, Context, Inputs, Message, Outputs, Streams};
use zenoh_flow::zenoh_flow_derive::ZFData;
use zenoh_flow::zfresult::ErrorKind;
use zenoh_flow::{bail, prelude::*};

// Data Type

#[derive(Debug, Clone, ZFData)]
pub struct ZFUsize(pub usize);

impl ZFData for ZFUsize {
    fn try_serialize(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_ne_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let value = usize::from_ne_bytes(
            bytes
                .try_into()
                .map_err(|e| zferror!(ErrorKind::DeseralizationError, "{}", e))?,
        );
        Ok(ZFUsize(value))
    }
}

static SOURCE: &str = "Counter";
static DESTINATION: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

// SOURCE

struct CountSource {
    rx: Receiver<()>,
    output: Output,
}

impl CountSource {
    fn make(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut outputs: Outputs,
        rx: Receiver<()>,
    ) -> Result<Self> {
        println!("[CountSource] constructor");
        let output = outputs.take(SOURCE).unwrap();

        Ok(CountSource { rx, output })
    }
}

struct CountFactory(Receiver<()>);

#[async_trait]
impl Factory for CountFactory {
    async fn make(
        &self,
        ctx: &mut Context,
        config: &Option<Configuration>,
        _inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Arc<dyn Node>> {
        CountSource::make(ctx, config, outputs, self.0.clone())
            .map(|node| Arc::new(node) as Arc<dyn Node>)
    }
}

#[async_trait]
impl Source for CountSource {
    fn new(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        _outputs: Outputs,
    ) -> Result<Self> {
        bail!(ErrorKind::Unsupported, "Use CountSource::make(..) instead")
    }
}

#[async_trait]
impl Node for CountSource {
    async fn iteration(&self) -> Result<()> {
        println!("[CountSource] iteration being");
        self.rx.recv_async().await.unwrap();

        COUNTER.fetch_add(1, Ordering::AcqRel);

        println!("[CountSource] sending on first output");
        self.output
            .send_async(ZFUsize(COUNTER.load(Ordering::Relaxed)), None)
            .await?;

        println!("[CountSource] iteration done");

        Ok(())
    }
}

// SINK

struct GenericSink {
    input: Input,
}

#[async_trait]
impl Sink for GenericSink {
    fn new(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Self> {
        println!("[GenericSink] constructor");
        let input = inputs.take(SOURCE).unwrap();

        Ok(GenericSink { input })
    }
}

#[async_trait]
impl Node for GenericSink {
    async fn iteration(&self) -> Result<()> {
        println!("[GenericSink] iteration being");
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<ZFUsize>()?;
            println!("[GenericSink] Data from first input {:?}", data);
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
        }

        println!("[GenericSink] iteration done");
        Ok(())
    }
}

struct SinkFactory();

#[async_trait]
impl Factory for SinkFactory {
    async fn make(
        &self,
        ctx: &mut Context,
        config: &Option<Configuration>,
        inputs: Inputs,
        _outputs: Outputs,
    ) -> Result<Arc<dyn Node>> {
        GenericSink::new(ctx, config, inputs).map(|node| Arc::new(node) as Arc<dyn Node>)
    }
}

// OPERATORS

struct NoOp {
    input: Input,
    output: Output,
}

#[async_trait]
impl Operator for NoOp {
    fn new(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[NoOp] constructor");
        Ok(NoOp {
            input: inputs.take(SOURCE).unwrap(),
            output: outputs.take(DESTINATION).unwrap(),
        })
    }
}

#[async_trait]
impl Node for NoOp {
    async fn iteration(&self) -> Result<()> {
        println!("[NoOp] iteration being");
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<ZFUsize>()?;
            println!("[NoOp] got data {:?}", data);
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
            self.output.send_async(data.clone(), None).await?;
            println!("[NoOp] sent data");
        }
        println!("[NoOp] iteration done");
        Ok(())
    }
}

struct NoOpFactory();

#[async_trait]
impl Factory for NoOpFactory {
    async fn make(
        &self,
        ctx: &mut Context,
        config: &Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Arc<dyn Node>> {
        NoOp::new(ctx, config, inputs, outputs).map(|node| Arc::new(node) as Arc<dyn Node>)
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx, rx) = bounded::<()>(1); // Channel used to trigger source

    let session = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .unwrap(),
    );
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let runtime_name: RuntimeId = format!("test-runtime-{}", rt_uuid).into();
    let ctx = RuntimeContext {
        session,
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: runtime_name.clone(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow = zenoh_flow::runtime::dataflow::DataFlow::new("test", ctx.clone());

    let source_record = SourceRecord {
        id: "counter-source".into(),
        uid: 0,
        outputs: vec![PortRecord {
            uid: 0,
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_source(source_record, Arc::new(CountFactory(rx)));

    let sink_record = SinkRecord {
        id: "generic-sink".into(),
        uid: 1,
        inputs: vec![PortRecord {
            uid: 2,
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_sink(sink_record, Arc::new(SinkFactory()));

    let no_op_record = OperatorRecord {
        id: "noop".into(),
        uid: 2,
        inputs: vec![PortRecord {
            uid: 4,
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
        outputs: vec![PortRecord {
            uid: 5,
            port_id: DESTINATION.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator(no_op_record, Arc::new(NoOpFactory()));

    dataflow.add_link(
        OutputDescriptor {
            node: "counter-source".into(),
            output: SOURCE.into(),
        },
        InputDescriptor {
            node: "noop".into(),
            input: SOURCE.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: "noop".into(),
            output: DESTINATION.into(),
        },
        InputDescriptor {
            node: "generic-sink".into(),
            input: SOURCE.into(),
        },
    );

    let mut instance = DataFlowInstance::try_instantiate(dataflow, hlc.clone())
        .await
        .unwrap();

    for id in instance.get_sinks() {
        instance.start_node(&id).unwrap();
    }

    for id in instance.get_operators() {
        instance.start_node(&id).unwrap();
    }

    for id in instance.get_sources() {
        instance.start_node(&id).unwrap();
    }

    tx.send_async(()).await.unwrap();

    async_std::task::sleep(std::time::Duration::from_secs(5)).await;

    for id in instance.get_sources() {
        instance.stop_node(&id).await.unwrap();
    }

    for id in instance.get_operators() {
        instance.stop_node(&id).await.unwrap();
    }

    for id in instance.get_sinks() {
        instance.stop_node(&id).await.unwrap();
    }
}

#[test]
fn run_single_runtime() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}
