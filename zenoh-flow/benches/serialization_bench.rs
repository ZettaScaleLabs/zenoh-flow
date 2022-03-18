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

use std::time::Duration;

use criterion::{black_box, Criterion};
use zenoh_flow::{Data, Message};

static MAX_PAYLOAD: u64 = 134_217_728; //128MB

fn bench_serialize(message: &Message) {
    let _ = message.serialize_bincode().unwrap();
}

fn bench_deserialize(data: &[u8]) {
    let _: Message = bincode::deserialize(data).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());

    let mut size = 8;

    while size <= MAX_PAYLOAD {
        let payload_data = (0u64..size).map(|i| (i % 10) as u8).collect::<Vec<u8>>();

        let data = Data::from_bytes(payload_data);
        let zf_msg = Message::from_serdedata(data, hlc.new_timestamp(), vec![], vec![]);

        let serialized_zf = zf_msg.clone().serialize_bincode().unwrap();

        c.bench_function(
            format!("serialize-zenoh-flow-message-{size}").as_str(),
            |b| {
                b.iter(|| {
                    let _ = bench_serialize(black_box(&zf_msg));
                })
            },
        );

        c.bench_function(
            format!("deserialize-zenoh-flow-message-{size}").as_str(),
            |b| {
                b.iter(|| {
                    let _ = bench_deserialize(black_box(&serialized_zf));
                })
            },
        );

        size *= 2;
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(60)).sample_size(500usize);
    targets = criterion_benchmark);
criterion_main!(benches);
