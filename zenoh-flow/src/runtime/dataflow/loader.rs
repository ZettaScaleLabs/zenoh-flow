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

use super::node::{OperatorFactory, SinkFactory, SourceFactory};
use crate::model::record::{OperatorRecord, SinkRecord, SourceRecord};
use crate::prelude::{Operator, Sink, Source};
use crate::runtime::dataflow::NodeFactoryFn;
use crate::types::Configuration;
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};
use serde::{Deserialize, Serialize};

use std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

use std::path::{Path, PathBuf};
use url::Url;

#[cfg(target_family = "unix")]
static LOAD_FLAGS: std::os::raw::c_int =
    libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL;

/// Constant used to check if a node is compatible with the currently running Zenoh Flow daemon.
/// As nodes are dynamically loaded, this is to prevent (possibly cryptic) runtime error due to
/// incompatible API.
pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Constant used to check if a node was compiled with the same version of the Rust compiler than
/// the currently running Zenoh Flow daemon.
/// As Rust is not ABI stable, this is to prevent (possibly cryptic) runtime errors.
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub static EXT_FILE_EXTENSION: &str = "zfext";

/// The alias to the function type created by the export macros.
pub(crate) type ExportFn<T> = fn() -> NodeDeclaration<T>;

/// FactorySymbol groups the symbol we must find in the shared library we load.
pub enum FactorySymbol {
    Source,
    Operator,
    Sink,
}

impl FactorySymbol {
    /// Returns the bytes representation of the symbol.
    ///
    /// They are of the form:
    ///
    /// `b"zf<node_kind>_factory_declaration\0"`
    ///
    /// Where `<node_kind>` is either `operator`, `source`, or `sink`.
    pub(crate) fn to_bytes(&self) -> &[u8] {
        match self {
            FactorySymbol::Source => b"_zf_export_source\0",
            FactorySymbol::Operator => b"_zf_export_operator\0",
            FactorySymbol::Sink => b"_zf_export_sink\0",
        }
    }
}

/// Declaration expected in the library that will be loaded.
pub struct NodeDeclaration<T: ?Sized> {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: NodeFactoryFn<T>,
}

/// Extensible support for different implementations
/// This represents the configuration for an extension.
///
///
/// Example:
///
/// ```yaml
/// name: python
/// file_extension: py
/// source_lib: ./target/release/libpy_source.so
/// sink_lib: ./target/release/libpy_sink.so
/// operator_lib: ./target/release/libpy_op.so
/// config_lib_key: python-script
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensibleImplementation {
    pub(crate) name: String,
    pub(crate) file_extension: String,
    pub(crate) source_lib: String,
    pub(crate) sink_lib: String,
    pub(crate) operator_lib: String,
    pub(crate) config_lib_key: String,
}

/// Loader configuration files, it includes the extensions.
///
/// Example:
///
/// ```yaml
/// extensions:
///   - name: python
///     file_extension: py
///     source_lib: ./target/release/libpy_source.so
///     sink_lib: ./target/release/libpy_sink.so
///     operator_lib: ./target/release/libpy_op.so
///     config_lib_key: python-script
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoaderConfig {
    extensions: Vec<ExtensibleImplementation>,
}

impl LoaderConfig {
    /// Creates an empty `LoaderConfig`.
    pub fn new() -> Self {
        Self { extensions: vec![] }
    }

    /// Adds the given extension.
    ///
    /// # Errors
    /// It returns an error variant if the extension is already present.
    pub fn try_add_extension(&mut self, ext: ExtensibleImplementation) -> Result<()> {
        if self.extensions.iter().any(|e| e.name == ext.name) {
            return Err(zferror!(ErrorKind::Duplicate).into());
        }
        self.extensions.push(ext);
        Ok(())
    }

    /// Removes the given extension.
    pub fn remove_extension(&mut self, name: &str) -> Option<ExtensibleImplementation> {
        if let Some(index) = self.extensions.iter().position(|e| e.name == name) {
            let ext = self.extensions.remove(index);
            return Some(ext);
        }
        None
    }

    /// Gets the extension that matches the given `file_extension`.
    pub fn get_extension_by_file_extension(
        &self,
        file_extension: &str,
    ) -> Option<&ExtensibleImplementation> {
        if let Some(ext) = self
            .extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
        {
            return Some(ext);
        }
        None
    }

    /// Gets the extension that matches the given `name`.
    pub fn get_extension_by_name(&self, name: &str) -> Option<&ExtensibleImplementation> {
        if let Some(ext) = self.extensions.iter().find(|e| e.name == name) {
            return Some(ext);
        }
        None
    }
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The dynamic library loader.
/// Before loading it verifies if the versions are compatible
/// and if the symbols are presents.
/// It loads the files in different way depending on the operating system.
/// In particular the scope of the symbols is different between Unix and
/// Windows.
/// In Unix system the symbols are loaded with the flags:
///
/// - `RTLD_NOW` load all the symbols when loading the library.
/// - `RTLD_LOCAL` keep all the symbols local.
pub struct Loader {
    pub(crate) config: LoaderConfig,
}

impl Loader {
    /// Creates a new `Loader` with the given `config`.
    pub fn new(config: LoaderConfig) -> Self {
        Self { config }
    }

    /// Loads a factory library, using one of the extension configured within the loader.
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different version of Zenoh-Flow used to build the factory
    /// - different version of the rust compiler used to build the factory
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is supported)
    /// - the extension is not known
    /// - the factory does not match the extension interface
    unsafe fn load_factory<T: ?Sized>(
        &self,
        factory_symbol: FactorySymbol,
        uri: &str,
        configuration: &mut Option<Configuration>,
    ) -> Result<(Library, NodeFactoryFn<T>)> {
        let uri = Url::parse(uri).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Missing file extension for < {:?} >",
                        file_path,
                    )
                })?;

                let library_path = if Self::is_lib(&file_extension) {
                    file_path
                } else {
                    match self.config.get_extension_by_file_extension(&file_extension) {
                        Some(e) => {
                            Self::wrap_configuration(
                                configuration,
                                e.config_lib_key.clone(),
                                &file_path,
                            )?;
                            let lib = match factory_symbol {
                                FactorySymbol::Source => &e.source_lib,
                                FactorySymbol::Operator => &e.operator_lib,
                                FactorySymbol::Sink => &e.sink_lib,
                            };
                            std::fs::canonicalize(lib)?
                        }
                        _ => bail!(ErrorKind::Unimplemented),
                    }
                };

                log::trace!("[Loader] loading library {:?}", library_path);

                #[cfg(target_family = "unix")]
                let library = Library::open(Some(library_path), LOAD_FLAGS)?;

                #[cfg(target_family = "windows")]
                let library = Library::new(library_path)?;

                let export_fn = library.get::<ExportFn<T>>(factory_symbol.to_bytes())?;
                let decl = (export_fn)();

                // version checks to prevent accidental ABI incompatibilities
                if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
                    return Err(zferror!(ErrorKind::VersionMismatch).into());
                }

                Ok((library, decl.register))
            }

            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Tries to load a SourceFactory from the information passed within the
    /// [`SourceRecord`](`SourceRecord`).
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different version of Zenoh-Flow used to build the source
    /// - different version of the rust compiler used to build the source
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is supported).
    pub(crate) fn load_source_factory(&self, mut record: SourceRecord) -> Result<SourceFactory> {
        if let Some(uri) = &record.uri {
            let (library, factory) = unsafe {
                self.load_factory::<dyn Source>(
                    FactorySymbol::Source,
                    uri,
                    &mut record.configuration,
                )?
            };

            Ok(SourceFactory::new_dynamic(
                record,
                factory,
                Arc::new(library),
            ))
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            )
        }
    }

    /// Tries to load an OperatorFactory from the information passed within the
    /// [`OperatorRecord`](`OperatorRecord`).
    ///
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - different versions of Zenoh-Flow used to build the operator
    /// - different versions of the rust compiler used to build the operator
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known).
    pub(crate) fn load_operator_factory(
        &self,
        mut record: OperatorRecord,
    ) -> Result<OperatorFactory> {
        if let Some(uri) = &record.uri {
            let (library, factory) = unsafe {
                self.load_factory::<dyn Operator>(
                    FactorySymbol::Operator,
                    uri,
                    &mut record.configuration,
                )?
            };

            Ok(OperatorFactory::new_dynamic(
                record,
                factory,
                Arc::new(library),
            ))
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Operator < {} >.",
                record.id.clone()
            )
        }
    }

    /// Tries to load a SinkFactory from the information passed within the
    /// [`SinkRecord`](`SinkRecord`).
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different versions of Zenoh-Flow used to build the sink
    /// - different versions of the rust compiler used to build the sink
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known).
    pub(crate) fn load_sink_factory(&self, mut record: SinkRecord) -> Result<SinkFactory> {
        if let Some(uri) = &record.uri {
            let (library, factory) = unsafe {
                self.load_factory::<dyn Sink>(FactorySymbol::Sink, uri, &mut record.configuration)?
            };

            Ok(SinkFactory::new_dynamic(record, factory, Arc::new(library)))
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Sink < {} >.",
                record.id.clone()
            )
        }
    }

    /// Converts the `Url` to a `PathBuf`
    fn make_file_path(uri: Url) -> Result<PathBuf> {
        let mut path = PathBuf::new();
        let file_path = match uri.host_str() {
            Some(h) => format!("{}{}", h, uri.path()),
            None => uri.path().to_string(),
        };
        path.push(file_path);
        let path = std::fs::canonicalize(&path)
            .map_err(|e| zferror!(ErrorKind::IOError, "{}: {}", e, &path.to_string_lossy()))?;
        Ok(path)
    }

    /// Checks if the file is a dynamic library.
    fn is_lib(ext: &str) -> bool {
        if ext == std::env::consts::DLL_EXTENSION {
            return true;
        }
        false
    }

    /// Returns the file extension, if any.
    fn get_file_extension(file: &Path) -> Option<String> {
        if let Some(ext) = file.extension() {
            if let Some(ext) = ext.to_str() {
                return Some(String::from(ext));
            }
        }
        None
    }

    /// Wraps the configuration in case of an extension.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - unable to parse the file path
    fn wrap_configuration(
        configuration: &mut Option<Configuration>,
        config_key: String,
        file_path: &Path,
    ) -> Result<()> {
        let mut new_config: serde_json::map::Map<String, Configuration> =
            serde_json::map::Map::new();
        let config = configuration.take();
        new_config.insert(
            config_key,
            file_path
                .to_str()
                .ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Unable parse file path < {:?} >.",
                        file_path,
                    )
                })?
                .into(),
        );

        if let Some(config) = config {
            new_config.insert(String::from("configuration"), config);
        }

        *configuration = Some(new_config.into());
        Ok(())
    }
}
