//! Common test utilities and helper functions.

use crate::parser::{extract_units, CodeUnit, Language};
use std::path::Path;

/// Helper to extract units from source code with a given language.
pub fn parse(source: &str, lang: Language, filename: &str) -> Vec<CodeUnit> {
    extract_units(Path::new(filename), source, lang)
}

/// Get the first unit with the given name.
pub fn get_unit_by_name<'a>(units: &'a [CodeUnit], name: &str) -> Option<&'a CodeUnit> {
    units.iter().find(|u| u.name == name)
}
