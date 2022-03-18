//
// Copyright (c) 2017, 2022 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
#[macro_use]
extern crate criterion;
use criterion::{black_box, Criterion};
use std::time::Duration;
use zenoh::net::protocol::io::*;
use zenoh::net::protocol::io::reader::HasReader;

//use zenoh_flow::runtime::message_new::{Data, Message};
use zenoh_flow::runtime::message_new::{
    deserialize_custom_zbuf, Data as NewData, DataMessage as NewDataMessage, ZenohFlowMessage,
};
use zenoh_flow::{Data, Message};

fn bench_serialize(message: &Message) {
    let _ = message.serialize_bincode().unwrap();
}

fn bench_serialize_custom(message: &ZenohFlowMessage, buff: &mut WBuf) {
    buff.clear();
    let _ = message.serialize_wbuf(buff);
}

fn bench_deserialize_custom(buff: &mut ZBuf, de_buf: &mut [u8] ) {
    buff.reader().reset();
    let _ = deserialize_custom_zbuf(buff, de_buf).unwrap();
}

fn bench_deserialize(data: &[u8]) {
    let _: Message = bincode::deserialize(data).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let payload_data: Vec<u8> = vec![];
    let data = Data::from_bytes(payload_data.clone());
    let zf_msg = Message::from_serdedata(data, hlc.new_timestamp(), vec![], vec![]);

    let serialized_zf = zf_msg.clone().serialize_bincode().unwrap();

    let data_new = NewData::from_bytes(payload_data);
    let data_msg_new = NewDataMessage::new(data_new, hlc.new_timestamp(), vec![]);
    let zf_msg_new = ZenohFlowMessage::Data(data_msg_new);

    let mut b_buff = WBuf::new(8192, false);

    let mut wbuff = WBuf::new(8192, false);

    let mut de_buff = Vec::with_capacity(65_535);

    zf_msg_new.clone().serialize_wbuf(&mut wbuff);

    let mut zbuf: ZBuf = wbuff.into();

    c.bench_function("bincode-serialize-zenoh-flow-message-data", |b| {
        b.iter(|| {
            let _ = bench_serialize(black_box(&zf_msg));
        })
    });

    c.bench_function("custom-serialize-zenoh-flow-message-data", |b| {
        b.iter(|| {
            let _ = bench_serialize_custom(black_box(&zf_msg_new), black_box(&mut b_buff));
        })
    });

    // c.bench_function("custom-vec-serialize-zenoh-flow-message-data", |b| {
    //     b.iter(|| {
    //         let _ = bench_serialize_custom(black_box(&zf_msg_new), black_box(&mut b_buff));
    //     })
    // });

    c.bench_function("bincode-deserialize-zenoh-flow-message-data", |b| {
        b.iter(|| {
            let _ = bench_deserialize(black_box(&serialized_zf));
        })
    });

    c.bench_function("custom-deserialize-zenoh-flow-message-data", |b| {
        b.iter(|| {
            let _ = bench_deserialize_custom(black_box(&mut zbuf), black_box(&mut de_buff));
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(60)).sample_size(1000usize);
    targets = criterion_benchmark);
criterion_main!(benches);
