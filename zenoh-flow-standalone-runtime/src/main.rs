//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::path::PathBuf;

use anyhow::Context;
use async_std::io::ReadExt;
use clap::Parser;
use zenoh_flow_commons::{parse_vars, Vars};
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{zenoh::AsyncResolve, Extensions, Runtime};

#[derive(Parser)]
struct Cli {
    /// The data flow to execute.
    flow: PathBuf,
    /// The path to a Zenoh configuration to manage the connection to the Zenoh
    /// network.
    ///
    /// If no configuration is provided, `zfctl` will default to connecting as
    /// a peer with multicast scouting enabled.
    #[arg(short = 'z', long, verbatim_doc_comment)]
    zenoh_configuration: Option<PathBuf>,
    /// The, optional, location of the configuration to load nodes implemented not in Rust.
    #[arg(short, long, value_name = "path")]
    extensions: Option<PathBuf>,
    /// Variables to add / overwrite in the `vars` section of your data
    /// flow, with the form `KEY=VALUE`. Can be repeated multiple times.
    ///
    /// Example:
    ///     --vars HOME_DIR=/home/zenoh-flow --vars BUILD=debug
    #[arg(long, value_parser = parse_vars::<String, String>, verbatim_doc_comment)]
    vars: Option<Vec<(String, String)>>,
}

#[async_std::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();
    let cli = Cli::parse();

    let extensions = match cli.extensions {
        Some(extensions_path) => {
            let (extensions, _) = zenoh_flow_commons::try_parse_from_file::<Extensions>(
                extensions_path.as_os_str(),
                Vars::default(),
            )
            .context(format!(
                "Failed to load Loader configuration from < {} >",
                &extensions_path.display()
            ))
            .unwrap();

            extensions
        }
        None => Extensions::default(),
    };

    let vars = match cli.vars {
        Some(v) => Vars::from(v),
        None => Vars::default(),
    };

    let (data_flow, vars) =
        zenoh_flow_commons::try_parse_from_file::<DataFlowDescriptor>(cli.flow.as_os_str(), vars)
            .context(format!(
                "Failed to load data flow descriptor from < {} >",
                &cli.flow.display()
            ))
            .unwrap();

    let flattened_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow, vars)
        .context(format!(
            "Failed to flattened data flow extracted from < {} >",
            &cli.flow.display()
        ))
        .unwrap();

    let mut runtime_builder = Runtime::builder("zenoh-flow-standalone-runtime")
        .add_extensions(extensions)
        .expect("Failed to add extensions");

    if let Some(path) = cli.zenoh_configuration {
        let zenoh_config = zenoh_flow_runtime::zenoh::Config::from_file(path.clone())
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                    path.display()
                )
            });
        let zenoh_session = zenoh_flow_runtime::zenoh::open(zenoh_config)
            .res_async()
            .await
            .unwrap_or_else(|e| panic!("Failed to open a Zenoh session: {e:?}"))
            .into_arc();

        runtime_builder = runtime_builder.session(zenoh_session);
    }

    let runtime = runtime_builder
        .build()
        .await
        .expect("Failed to build the Zenoh-Flow runtime");

    let record = DataFlowRecord::try_new(&flattened_flow, runtime.id())
        .context("Failed to create a Record from the flattened data flow descriptor")
        .unwrap();

    let instance_id = record.instance_id().clone();
    let record_name = record.name().clone();
    runtime
        .try_load_data_flow(record)
        .await
        .context("Failed to load Record")
        .unwrap();

    runtime
        .try_start_instance(&instance_id)
        .await
        .unwrap_or_else(|e| panic!("Failed to start data flow < {} >: {:?}", &instance_id, e));

    let mut stdin = async_std::io::stdin();
    let mut input = [0_u8];
    println!(
        r#"
    The flow ({}) < {} > was successfully started.

    To abort its execution, simply enter 'q'.
    "#,
        record_name, instance_id
    );

    loop {
        let _ = stdin.read_exact(&mut input).await;
        if input[0] == b'q' {
            break;
        }
    }

    runtime
        .try_delete_instance(&instance_id)
        .await
        .unwrap_or_else(|e| panic!("Failed to delete data flow < {} >: {:?}", &instance_id, e));
}
