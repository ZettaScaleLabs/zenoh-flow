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

/// Spin lock over an [`async_std::sync::Mutex`](`async_std::sync::Mutex`)
/// Note: This is intended for internal usage.
#[macro_export]
macro_rules! zf_spin_lock {
    ($val : expr) => {
        loop {
            match $val.try_lock() {
                Some(x) => break x,
                None => std::hint::spin_loop(),
            }
        }
    };
}

/// This macro exports the symbol Zenoh-Flow will look for when dynamically loading the library of a
/// [`Source`](`Source`) node.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySource {
///     output: Output<usize>,
/// }
///
/// #[async_trait]
/// impl Node for MySource {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Source for MySource {
///     async fn new(
///         context: Context,
///         configuration: Option<Configuration>,
///         outputs: Outputs,
///     ) -> Result<Self> {
///         todo!()
///     }
/// }
///
/// export_source!(MySource);
/// ```
#[macro_export]
macro_rules! export_source {
    ($source:ty) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_source: $crate::runtime::dataflow::loader::NodeDeclaration<
            $crate::runtime::dataflow::node::SourceFn,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<
            $crate::runtime::dataflow::node::SourceFn,
        > {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: $crate::types::Context,
                          configuration: Option<$crate::types::Configuration>,
                          outputs: $crate::io::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <$source>::new(context, configuration, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn $crate::traits::Node>)
                })
            },
        };
    };
}

/// This macro exports the symbol Zenoh-Flow will look for when dynamically loading the library of
/// an [`Operator`](`Operator`) node.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// pub struct MyOperator {
///     input: Input<usize>,
///     output: Output<String>,
/// }
///
/// #[async_trait]
/// impl Node for MyOperator {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Operator for MyOperator {
///     async fn new(
///         context: Context,
///         configuration: Option<Configuration>,
///         inputs: Inputs,
///         outputs: Outputs,
///     ) -> Result<Self> {
///         todo!()
///     }
/// }
///
/// export_operator!(MyOperator);
/// ```
#[macro_export]
macro_rules! export_operator {
    ($operator:ty) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_operator: $crate::runtime::dataflow::loader::NodeDeclaration<
            $crate::runtime::dataflow::node::OperatorFn,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<
            $crate::runtime::dataflow::node::OperatorFn,
        > {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: $crate::types::Context,
                          configuration: Option<$crate::types::Configuration>,
                          mut inputs: $crate::io::Inputs,
                          mut outputs: $crate::io::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <$operator>::new(context, configuration, inputs, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn $crate::traits::Node>)
                })
            },
        };
    };
}

/// This macro exports the symbol Zenoh-Flow will look for when dynamically loading the library of a
/// [`Sink`](`Sink`) node.
///
/// # Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySink {
///     input: Input<String>,
/// }
///
/// #[async_trait]
/// impl Node for MySink {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Sink for MySink {
///     async fn new(
///         context: Context,
///         configuration: Option<Configuration>,
///         inputs: Inputs,
///     ) -> Result<Self> {
///         todo!()
///     }
/// }
///
/// export_sink!(MySink);
/// ```
#[macro_export]
macro_rules! export_sink {
    ($sink:ty) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_sink: $crate::runtime::dataflow::loader::NodeDeclaration<
            $crate::runtime::dataflow::node::SinkFn,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<
            $crate::runtime::dataflow::node::SinkFn,
        > {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: $crate::types::Context,
                          configuration: Option<$crate::types::Configuration>,
                          mut inputs: $crate::io::Inputs| {
                std::boxed::Box::pin(async {
                    let node = <$sink>::new(context, configuration, inputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn $crate::traits::Node>)
                })
            },
        };
    };
}
