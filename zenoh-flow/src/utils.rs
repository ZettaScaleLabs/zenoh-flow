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

use crate::prelude::ErrorKind;
use crate::{bail, zferror, Result};
use std::path::{Path, PathBuf};
use url::Url;

/// Given a string representing a [`Url`](`url::Url`), transform it into a
/// [`PathBuf`](`std::path::PathBuf`).
///
/// Only one scheme, for now, is supported: `file://`. This will change in upcoming releases of
/// Zenoh-Flow.
///
/// # Errors
///
/// This function will return an error in the following situations:
/// - The provided string does not match the syntax of a [`Url`](`url::Url`).
/// - The scheme used for the url is NOT `file://`.
/// - The resulting path cannot be [`canonicalized`](`std::fs::canonicalize`).
pub(crate) fn try_make_file_path(url_str: &str) -> Result<PathBuf> {
    let uri = Url::parse(url_str).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

    if uri.scheme() != "file" {
        bail!(
            ErrorKind::ParsingError,
            "Only the 'file://' scheme is supported, found: {}://",
            uri.scheme()
        );
    }

    let mut path = PathBuf::new();
    #[cfg(test)]
    {
        path.push(env!("CARGO_MANIFEST_DIR"));
    }

    let file_path = match uri.host_str() {
        Some(h) => format!("{}{}", h, uri.path()),
        None => uri.path().to_string(),
    };
    path.push(file_path);
    let path = std::fs::canonicalize(&path)
        .map_err(|e| zferror!(ErrorKind::IOError, "{}: {}", e, &path.to_string_lossy()))?;
    Ok(path)
}

/// Returns the file extension, if any.
pub(crate) fn get_file_extension(file: &Path) -> Option<String> {
    if let Some(ext) = file.extension() {
        if let Some(ext) = ext.to_str() {
            return Some(String::from(ext));
        }
    }
    None
}

/// Checks if the provided extension is that of a [`dynamic
/// library`](`std::env::consts::DLL_EXTENSION`).
pub(crate) fn is_dynamic_library(ext: &str) -> bool {
    if ext == std::env::consts::DLL_EXTENSION {
        return true;
    }
    false
}
