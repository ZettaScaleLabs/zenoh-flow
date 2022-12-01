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
use zenoh_flow::{prelude::*, bail};
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::traits::{
    Node, OperatorFactoryTrait, SinkFactoryTrait, SourceFactoryTrait, ZFData,
};
use zenoh_flow::types::{Configuration, Context, Inputs, Message, Outputs, Streams};
use zenoh_flow::zenoh_flow_derive::ZFData;
use zenoh_flow::zfresult::ErrorKind;

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
                .map_err(|e| zferror!(ErrorKind::DeserializationError, "{}", e))?,
        );
        Ok(ZFUsize(value))
    }
}

static SOURCE: &str = "Counter";
static DESTINATION: &str = "Counter";
static PORT_CALLBACK: &str = "Counter_callback";

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static COUNTER_CALLBACK: AtomicUsize = AtomicUsize::new(1);

// SOURCE

struct CountSource {
    rx: Receiver<()>,
    output: Output,
    output_callback: Output,
}

impl CountSource {
    fn make(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut outputs: Outputs,
        rx: Receiver<()>,
    ) -> Result<Option<Self>> {
        let output = outputs.take(SOURCE).unwrap();
        let output_callback = outputs.take(PORT_CALLBACK).unwrap();

        Ok(Some(CountSource {
            rx,
            output,
            output_callback,
        }))
    }
}

#[async_trait]
impl Source for CountSource {
    fn new(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut outputs: Outputs,
    ) -> Result<Option<Self>> {
        bail!(ErrorKind::Unsupported, "Use CountSource::make(..) instead")
    }
    async fn iteration(&self) -> Result<()> {
        self.rx.recv_async().await.unwrap();



        COUNTER.fetch_add(1, Ordering::AcqRel);
        self.output
            .send_async(ZFUsize(COUNTER.load(Ordering::Relaxed)), None)
            .await?;

        COUNTER_CALLBACK.fetch_add(1, Ordering::AcqRel);
        self.output_callback
            .send_async(ZFUsize(COUNTER_CALLBACK.load(Ordering::Relaxed)), None)
            .await
    }
}

// SINK

struct GenericSink {
    input: Input,
    input_callback: Input,
}

#[async_trait]
impl Sink for GenericSink {
    fn new(
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Option<Self>> {
        let input = inputs.take(SOURCE).unwrap();
        let input_callback = inputs.take(PORT_CALLBACK).unwrap();

        Ok(Some(GenericSink {
            input,
            input_callback,
        }))
    }

    async fn iteration(&self) -> Result<()> {
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
        }

        if let Ok(Message::Data(mut msg)) = self.input_callback.recv_async().await {
            let data = msg.try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER_CALLBACK.load(Ordering::Relaxed));
        }

        Ok(())
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
    ) -> Result<Option<Self>> {
        Ok(Some(NoOp {
            input: inputs.take(SOURCE).unwrap(),
            output: outputs.take(DESTINATION).unwrap(),
        }))
    }

    async fn iteration(&self) -> Result<()> {
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
            self.output.send_async(data.clone(), None).await?;
        }
        Ok(())
    }
}

struct NoOpCallback();

#[async_trait]
impl Operator for NoOpCallback {
    fn new(
        context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Option<Self>> {
        let input = inputs.take(PORT_CALLBACK).unwrap();
        let output = outputs.take_into_arc(PORT_CALLBACK).unwrap();

        context.register_input_callback(
            input,
            Arc::new(move |message| {
                let output_cloned = output.clone();

                async move {
                    println!("Entering callback");
                    let data = match message {
                        Message::Data(mut data) => data.try_get::<ZFUsize>()?.clone(),
                        _ => return Err(zferror!(ErrorKind::Unsupported).into()),
                    };

                    assert_eq!(data.0, COUNTER_CALLBACK.load(Ordering::Relaxed));
                    output_cloned.send_async(data, None).await?;
                    Ok(())
                }
            }),
        );

        Ok(None)
    }

    async fn iteration(&self) -> Result<()> {
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
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
        outputs: vec![
            PortRecord {
                uid: 0,
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 1,
                port_id: PORT_CALLBACK.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_source_factory_new(
        source_record,
        Arc::new(move |ctx, config, _inputs, mut outputs| {
            match CountSource::make(ctx, config, outputs, rx.clone()) {
                Ok(Some(source)) => Ok(Some(Arc::new(source) as Arc<dyn Source>)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        }
    ));
    // dataflow.add_source_factory(source_record, Arc::new(CountSourceFactory { rx }));

    let sink_record = SinkRecord {
        id: "generic-sink".into(),
        uid: 1,
        inputs: vec![
            PortRecord {
                uid: 2,
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 3,
                port_id: PORT_CALLBACK.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    // dataflow.add_sink_factory(sink_record, Arc::new(GenericSinkFactory));
    dataflow.add_sink_factory_new(
        sink_record,
        Arc::new(move |ctx, config, inputs, _| {
            match GenericSink::new(ctx, config, inputs) {
                Ok(Some(sink)) => Ok(Some(Arc::new(sink) as Arc<dyn Sink>)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        },
    ));

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

    // dataflow.add_operator_factory(no_op_record, Arc::new(NoOpFactory));
    dataflow.add_operator_factory_new(
        no_op_record,
        Arc::new(move |ctx, config, inputs, outputs|  {
            match NoOp::new(ctx, config, inputs, outputs) {
                Ok(Some(op)) => Ok(Some(Arc::new(op) as Arc<dyn Operator>)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        },
    ));

    let no_op_callback_record = OperatorRecord {
        id: "noop_callback".into(),
        uid: 3,
        inputs: vec![PortRecord {
            uid: 6,
            port_id: PORT_CALLBACK.into(),
            port_type: "int".into(),
        }],
        outputs: vec![PortRecord {
            uid: 7,
            port_id: PORT_CALLBACK.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    // dataflow.add_operator_factory(no_op_callback_record, Arc::new(NoOpCallbackFactory));
    dataflow.add_operator_factory_new(
        no_op_callback_record,
        Arc::new(move |ctx, config, inputs, outputs| {
            match NoOpCallback::new(ctx, config, inputs, outputs) {
                Ok(Some(op)) => Ok(Some(Arc::new(op) as Arc<dyn Operator>)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        },
    ));

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

    dataflow.add_link(
        OutputDescriptor {
            node: "counter-source".into(),
            output: PORT_CALLBACK.into(),
        },
        InputDescriptor {
            node: "noop_callback".into(),
            input: PORT_CALLBACK.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: "noop_callback".into(),
            output: PORT_CALLBACK.into(),
        },
        InputDescriptor {
            node: "generic-sink".into(),
            input: PORT_CALLBACK.into(),
        },
    );

    let mut instance = DataFlowInstance::try_instantiate_new(dataflow, hlc.clone())
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

    async_std::task::sleep(std::time::Duration::from_secs(2)).await;

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
