use std::collections::HashMap;

use super::field;
use value::Kind;

/// The input schema for a given component.
///
/// This schema defines the (semantic) fields a component expects to receive from its input
/// components.
#[derive(Debug, Clone)]
pub struct Input {
    fields: HashMap<field::Purpose, Kind>,
}

impl Input {
    /// Create a new empty schema.
    ///
    /// An empty schema is the most "open" schema, in that there are no restrictions.
    pub fn empty() -> Self {
        Self {
            fields: HashMap::default(),
        }
    }

    pub fn purposes(&self) -> Vec<&field::Purpose> {
        self.fields.keys().collect()
    }

    /// Add a restriction to the schema.
    pub fn require_field_purpose(&mut self, purpose: impl Into<field::Purpose>, kind: Kind) {
        self.fields.insert(purpose.into(), kind);
    }
}

impl IntoIterator for Input {
    type Item = (field::Purpose, Kind);
    type IntoIter = std::collections::hash_map::IntoIter<field::Purpose, Kind>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}
