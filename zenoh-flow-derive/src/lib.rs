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

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};
/// The `ZFData` derive macro is provided to help the users
/// in implementing the `DowncastAny` trait.
///
/// Example::
/// ```no_compile
/// use zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct ZFString(pub String);
/// ```
#[proc_macro_derive(ZFData)]
pub fn zf_data_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let gen = quote! {

        impl zenoh_flow::prelude::DowncastAny for #ident {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
    gen.into()
}

/// The `ZFSource` derive macro is provided to allow the users
/// in exporting their source.
///
/// Example::
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFSource;
///
/// #[derive(ZFSource)]
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
/// }
///
/// #[async_trait]
/// impl Node for MySource {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_derive(ZFSource)]
pub fn zf_source_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let factory_ident = format_ident!("Factory{}", ident);

    let gen = quote! {




        #[doc(hidden)]
        struct #factory_ident();

        #[doc(hidden)]
        #[async_trait::async_trait]
        impl zenoh_flow::traits::Factory for #factory_ident {
            async fn make(
                &self,
                ctx: &mut zenoh_flow::prelude::Context,
                config: &Option<zenoh_flow::prelude::Configuration>,
                _inputs: zenoh_flow::prelude::Inputs,
                outputs: zenoh_flow::prelude::Outputs,
            ) -> Result<Option<Arc<dyn Node>>> {
                match #ident::new(ctx, config, outputs) {
                    Ok(Some(source)) => Ok(Some(
                        std::sync::Arc::new(source) as Arc<dyn zenoh_flow::traits::Node>
                    )),
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        }

        #[doc(hidden)]
        #[no_mangle]
        fn _zf_export_source() -> zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
            zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
                rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
                register: std::sync::Arc::new(#factory_ident()),
            }
        }
    };
    gen.into()
}

/// The `ZFSink` derive macro is provided to allow the users
/// in exporting their sink.
///
/// Example::
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFSink;
///
/// #[derive(ZFSink)]
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
/// }
///
/// #[async_trait]
/// impl Node for MySink {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_derive(ZFSink)]
pub fn zf_sink_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let factory_ident = format_ident!("Factory{}", ident);

    let gen = quote! {




        #[doc(hidden)]
        struct #factory_ident();

        #[doc(hidden)]
        #[async_trait::async_trait]
        impl zenoh_flow::traits::Factory for #factory_ident {
            async fn make(
                &self,
                ctx: &mut zenoh_flow::prelude::Context,
                config: &Option<zenoh_flow::prelude::Configuration>,
                inputs: zenoh_flow::prelude::Inputs,
                _outputs: zenoh_flow::prelude::Outputs,
            ) -> Result<Option<Arc<dyn Node>>> {
                match #ident::new(ctx, config, inputs) {
                    Ok(Some(sink)) => Ok(Some(
                        std::sync::Arc::new(sink) as Arc<dyn zenoh_flow::traits::Node>
                    )),
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        }

        #[doc(hidden)]
        #[no_mangle]
        fn _zf_export_sink() -> zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
            zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
                rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
                register: std::sync::Arc::new(#factory_ident()),
            }
        }
    };
    gen.into()
}

/// The `ZFOperator` derive macro is provided to allow the users
/// in exporting their operator.
///
/// Example::
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFOperator;
///
/// #[derive(ZFOperator)]
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
///    Self: Sized {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Node for MyOperator {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_derive(ZFOperator)]
pub fn zf_operator_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let factory_ident = format_ident!("Factory{}", ident);

    let gen = quote! {




        #[doc(hidden)]
        struct #factory_ident();

        #[doc(hidden)]
        #[async_trait::async_trait]
        impl zenoh_flow::traits::Factory for #factory_ident {
            async fn make(
                &self,
                ctx: &mut zenoh_flow::prelude::Context,
                config: &Option<zenoh_flow::prelude::Configuration>,
                inputs: zenoh_flow::prelude::Inputs,
                outputs: zenoh_flow::prelude::Outputs,
            ) -> Result<Option<Arc<dyn Node>>> {
                match #ident::new(ctx, config, inputs, outputs) {
                    Ok(Some(operator)) => Ok(Some(
                        std::sync::Arc::new(operator) as Arc<dyn zenoh_flow::traits::Node>
                    )),
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        }

        #[doc(hidden)]
        #[no_mangle]
        fn _zf_export_operator() -> zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
            zenoh_flow::runtime::dataflow::loader::NodeDeclaration {
                rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
                register: std::sync::Arc::new(#factory_ident()),
            }
        }
    };
    gen.into()
}
