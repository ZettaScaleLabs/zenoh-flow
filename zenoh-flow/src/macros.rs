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
/// pub struct MyOperator;
///
/// #[async_trait]
/// impl Operator for MyOperator {
///     fn new(
///         context: &mut Context,
///         configuration: &Option<Configuration>,
///         inputs: Inputs,
///         outputs: Outputs,
/// ) -> Result<Option<Self>>
///    where
///    Self: Sized; {
///         todo!()
///     }
///
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// export_operator!(MyOperator);
/// ```
///
#[macro_export]
macro_rules! export_operator {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_operator: $crate::runtime::dataflow::loader::NodeDeclaration<
        $crate::traits::Operator,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<$crate::traits::Operator> {
            rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
            register: Arc::new(|ctx, configuration, inputs, outputs| {
                match $factory::new(ctx, configuration, inputs, outputs) {
                    Ok(Some(operator)) => Ok(Some(Arc::new(operator) as Arc<dyn $crate::traits::Operator)),
                    Ok(None) => None,
                    Err(e) => Err(e),
                }
            }),
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
/// pub struct MySink;
///
/// #[async_trait]
/// impl Sink for MySink {
///   fn new(
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       inputs: Inputs,
///   ) -> Result<Option<Self>> {
///         todo!()
///     }
///
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
///
/// export_sink!(MySink);
/// ```
///
#[macro_export]
macro_rules! export_sink {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_sink: $crate::runtime::dataflow::loader::NodeDeclaration<Sink> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<$crate::traits::Sink>> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: std::sync::Arc::new(|ctx, configuration, inputs, _| {
                    match $factory::new(ctx, configuration, inputs) {
                        Ok(Some(source)) => Ok(Some(std::sync::Arc::new(source) as Arc<dyn $crate::traits::Sink>))
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    }
                }),
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
/// pub struct MySource;
///
/// #[async_trait]
/// impl Source for MySource{
///   fn new(
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       outputs: Outputs,
///   ) -> Result<Option<Self>> {
///         todo!()
///     }
///
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// export_source!(MySource);
/// ```
///
#[macro_export]
macro_rules! export_source {
    ($operator:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_source: $crate::runtime::dataflow::loader::NodeDeclaration<$crate::traits::Source> =
            $crate::runtime::dataflow::loader::NodeDeclaration::<$crate::traits::Source> {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: std::sync::Arc::new(|ctx, configuration, _, outputs| {
                    match $factory::new(ctx, configuration, outputs) {
                        Ok(Some(source)) => Ok(Some(std::sync::Arc::new(source) as Arc<dyn $crate::traits::Source)),
                        Ok(None) => None,
                        Err(e) => Err(e),
                    }
                }),
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
