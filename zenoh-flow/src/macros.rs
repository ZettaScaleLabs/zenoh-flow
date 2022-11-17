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

//TODO:docs
#[macro_export]
macro_rules! export_operator {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_operator: $crate::runtime::dataflow::loader::NodeDeclaration<
            Operator,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<Operator> {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            register: |ctx, configuration, inputs, outputs| {
                std::sync::Arc::new($factory::new(ctx, configuration, inputs, outputs))
            },
        };
    };
}

//TODO:docs
#[macro_export]
macro_rules! export_sink {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_sink: $crate::runtime::dataflow::loader::NodeDeclaration<Sink> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<Sink> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: |ctx, configuration, inputs, _| {
                    std::sync::Arc::new($factory::new(ctx, configuration, inputs))
                },
            };
    };
}

//TODO:docs
#[macro_export]
macro_rules! export_source {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_source: $crate::runtime::dataflow::loader::NodeDeclaration<Source> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<Source> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: |ctx, configuration, _, outputs| {
                    std::sync::Arc::new($factory::new(ctx, configuration, outputs))
                },
            };
    };
}

/// This macros should be used in order to provide the symbols
/// for the dynamic load of an Operator. Along with a register function
///
/// Example:
///
/// ```no_run
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MyOperatorFactory;
///
/// #[async_trait]
/// impl OperatorFactoryTrait for MyOperatorFactory {
/// async fn new_operator(
///     &self,
///     context: &mut Context,
///     configuration: &Option<Configuration>,
///     inputs: Inputs,
///     outputs: Outputs,
/// ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_operator_factory!(MyOperatorFactory {});
/// ```
///
#[macro_export]
macro_rules! export_operator_factory {
    ($factory:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_factory_declaration:
            $crate::runtime::dataflow::loader::NodeDeclaration<OperatorFactoryTrait> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<OperatorFactoryTrait> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: || std::sync::Arc::new($factory),
            };
    };
}

/// This macros should be used in order to provide the symbols
/// for the dynamic load of an Source. Along with a register function
///
/// Example:
///
/// ```no_run
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySourceFactory;
///
/// #[async_trait]
/// impl SourceFactoryTrait for MySourceFactory {
///   async fn new_source(
///       &self,
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       outputs: Outputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_source_factory!(MySourceFactory {});
/// ```
///
#[macro_export]
macro_rules! export_source_factory {
    ($factory:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsource_factory_declaration:
            $crate::runtime::dataflow::loader::NodeDeclaration<SourceFactoryTrait> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<SourceFactoryTrait> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: || std::sync::Arc::new($factory),
            };
    };
}

/// This macros should be used in order to provide the symbols
/// for the dynamic load of a Sink. Along with a register function
///
/// Example:
///
/// ```no_run
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySinkFactory;
///
/// #[async_trait]
/// impl SinkFactoryTrait for MySinkFactory {
///   async fn new_sink(
///       &self,
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       inputs: Inputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_sink_factory!(MySinkFactory {});
/// ```
///
#[macro_export]
macro_rules! export_sink_factory {
    ($factory:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsink_factory_declaration: $crate::runtime::dataflow::loader::NodeDeclaration<
            SinkFactoryTrait,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<SinkFactoryTrait> {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            register: || std::sync::Arc::new($factory),
        };
    };
}

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
