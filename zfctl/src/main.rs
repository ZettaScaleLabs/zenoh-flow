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

mod instance_command;
use instance_command::InstanceCommand;

mod runtime_command;
use runtime_command::RuntimeCommand;

mod utils;
use std::path::PathBuf;

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use utils::{get_random_runtime, get_runtime_by_name};
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{Result, RuntimeId};

const ZENOH_FLOW_INTERNAL_ERROR: &str = r#"
`zfctl` encountered a fatal internal error.

If the above error log does not help you troubleshoot the reason, you can contact us on:
- Discord:  https://discord.gg/CeJB5rxk9x
- GitHub:   https://github.com/eclipse-zenoh/zenoh-flow
"#;

/// Macro to facilitate the creation of a [Row](comfy_table::Row) where its contents are not of the same type.
#[macro_export]
macro_rules! row {
    (
        $( $cell: expr ),*
    ) => {
        comfy_table::Row::from(vec![ $( &$cell as &dyn std::fmt::Display ),*])
    };
}

#[derive(Parser)]
struct Zfctl {
    /// The path to a Zenoh configuration to manage the connection to the Zenoh
    /// network.
    ///
    /// If no configuration is provided, `zfctl` will default to connecting as
    /// a peer with multicast scouting enabled.
    #[arg(short = 'z', long, verbatim_doc_comment)]
    zenoh_configuration: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// To manage a data flow instance.
    ///
    /// This command accepts an optional `name` or `id` of a Zenoh-Flow Runtime
    /// to contact. If no name or id is provided, one is randomly selected.
    #[group(required = false, multiple = false)]
    Instance {
        #[command(subcommand)]
        command: InstanceCommand,
        /// The unique identifier of the Zenoh-Flow runtime to contact.
        #[arg(short = 'i', long = "id", verbatim_doc_comment, group = "runtime")]
        runtime_id: Option<RuntimeId>,
        /// The name of the Zenoh-Flow runtime to contact.
        ///
        /// If several runtimes share the same name, `zfctl` will abort
        /// its execution asking you to instead use their `id`.
        #[arg(short = 'n', long = "name", verbatim_doc_comment, group = "runtime")]
        runtime_name: Option<String>,
    },

    /// To interact with a Zenoh-Flow runtime.
    #[command(subcommand)]
    Runtime(RuntimeCommand),
}

#[async_std::main]
async fn main() -> Result<()> {
    // TODO Configure tracing such that:
    // - if the environment variable RUST_LOG is set, it is applied,
    // let a = std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV);
    // - otherwise, provide a default that will only log INFO or above messages, for zfctl only.
    let _ = tracing_subscriber::fmt::try_init();

    let zfctl = Zfctl::parse();

    let zenoh_config = match zfctl.zenoh_configuration {
        Some(path) => zenoh::prelude::Config::from_file(path.clone()).map_err(|e| {
            anyhow!(
                "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                path.display()
            )
        })?,
        None => zenoh::config::peer(),
    };

    let session = zenoh::open(zenoh_config)
        .res()
        .await
        .map_err(|e| anyhow!("Failed to open Zenoh session:\n{:?}", e))?;

    match zfctl.command {
        Command::Instance {
            command,
            runtime_id,
            runtime_name,
        } => {
            let orchestrator_id = match (runtime_id, runtime_name) {
                (Some(id), _) => id,
                (None, Some(name)) => get_runtime_by_name(&session, &name).await,
                (None, None) => get_random_runtime(&session).await,
            };

            command.run(session, orchestrator_id).await
        }
        Command::Runtime(command) => command.run(&session).await,
    }
}
