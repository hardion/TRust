//! Properties (RFC-5): declarations, typed values, naming rule.

use crate::types::{DataType, DevFailed, DevResult, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A device property declared by a device class.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PropInfo {
    pub name: String,
    pub data_type: DataType,
    pub description: String,
    /// Used when the Database has no value; `None` means no default.
    pub default: Option<Vec<String>>,
    /// The device is FAULT when the Database has no value (RFC-5: no default allowed).
    pub mandatory: bool,
}

/// The typed property values handed to `Device::init`, looked up case-insensitively.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties(HashMap<String, Value>);

impl Properties {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(&name.to_lowercase())
    }

    /// Resolves the declared properties from raw string lists (device > class > default).
    /// Returns the values, and the names of the missing mandatory properties.
    pub(crate) fn resolve(
        decl: &[PropInfo],
        lookup: impl Fn(&str) -> Option<Vec<String>>,
    ) -> DevResult<(Self, Vec<String>)> {
        let (mut values, mut missing) = (HashMap::new(), vec![]);
        for p in decl {
            match lookup(&p.name).or_else(|| p.default.clone()) {
                Some(raw) => drop(values.insert(p.name.to_lowercase(), parse_value(p.data_type, &raw)?)),
                None if p.mandatory => missing.push(p.name.clone()),
                None => {}
            }
        }
        Ok((Self(values), missing))
    }
}

fn bad(t: DataType, raw: &[String]) -> DevFailed {
    DevFailed::new("API_IncompatiblePropertyType", format!("{raw:?} is not a {t:?}"), "Properties")
}

/// Converts the strings of a property into a value of `t` (RFC-5 recommended types).
pub fn parse_value(t: DataType, raw: &[String]) -> DevResult<Value> {
    macro_rules! scalar {
        ($v:ident) => {
            Value::$v(raw.first().ok_or_else(|| bad(t, raw))?.trim().parse().map_err(|_| bad(t, raw))?)
        };
    }
    macro_rules! array {
        ($v:ident) => {
            Value::$v(raw.iter().map(|s| s.trim().parse()).collect::<Result<_, _>>().map_err(|_| bad(t, raw))?)
        };
    }
    Ok(match t {
        DataType::Boolean => match raw.first().map(|s| s.trim().to_lowercase()).as_deref() {
            Some("true" | "1") => Value::Boolean(true),
            Some("false" | "0") => Value::Boolean(false),
            _ => return Err(bad(t, raw)),
        },
        DataType::Short => scalar!(Short),
        DataType::UShort => scalar!(UShort),
        DataType::Long => scalar!(Long),
        DataType::ULong => scalar!(ULong),
        DataType::Long64 => scalar!(Long64),
        DataType::ULong64 => scalar!(ULong64),
        DataType::Float => scalar!(Float),
        DataType::Double => scalar!(Double),
        DataType::String => Value::String(raw.join("\n")),
        DataType::ShortArray => array!(ShortArray),
        DataType::LongArray => array!(LongArray),
        DataType::Long64Array => array!(Long64Array),
        DataType::FloatArray => array!(FloatArray),
        DataType::DoubleArray => array!(DoubleArray),
        DataType::StringArray => Value::StringArray(raw.to_vec()),
        _ => return Err(bad(t, raw)),
    })
}

/// RFC-5: `ALPHA *254(alphanum / "_")`; attribute properties may also start with `_`.
pub fn valid_property_name(name: &str, attribute: bool) -> bool {
    let mut c = name.chars();
    let first = c.next().is_some_and(|f| f.is_ascii_alphabetic() || (attribute && f == '_'));
    first && name.len() <= 255 && c.all(|x| x.is_ascii_alphanumeric() || x == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn typed_values() {
        assert_eq!(parse_value(DataType::Double, &v(&["1.5"])).unwrap(), Value::Double(1.5));
        assert!(matches!(parse_value(DataType::Double, &v(&["NaN"])).unwrap(), Value::Double(x) if x.is_nan()));
        assert_eq!(parse_value(DataType::Double, &v(&["-inf"])).unwrap(), Value::Double(f64::NEG_INFINITY));
        assert_eq!(parse_value(DataType::Boolean, &v(&["True"])).unwrap(), Value::Boolean(true));
        assert_eq!(parse_value(DataType::LongArray, &v(&["1", " 2"])).unwrap(), Value::LongArray(vec![1, 2]));
        assert_eq!(parse_value(DataType::Short, &v(&["70000"])).unwrap_err().reason(), "API_IncompatiblePropertyType");
        assert!(parse_value(DataType::Long, &[]).is_err());
    }

    #[test]
    fn precedence_and_mandatory() {
        let decl = [
            PropInfo {
                name: "Speed".into(),
                data_type: DataType::Double,
                default: Some(v(&["1"])),
                ..Default::default()
            },
            PropInfo { name: "Port".into(), data_type: DataType::String, mandatory: true, ..Default::default() },
        ];
        let (p, missing) = Properties::resolve(&decl, |_| None).unwrap();
        assert_eq!((p.get("speed"), missing), (Some(&Value::Double(1.0)), vec!["Port".to_string()]));
        let (p, missing) = Properties::resolve(&decl, |n| Some(v(&[if n == "Speed" { "2" } else { "x" }]))).unwrap();
        assert_eq!((p.get("SPEED"), missing.len()), (Some(&Value::Double(2.0)), 0));
    }

    #[test]
    fn names() {
        assert!(valid_property_name("host_1", false));
        assert!(!valid_property_name("1host", false) && !valid_property_name("_x", false));
        assert!(valid_property_name("__value", true));
        assert!(!valid_property_name("a-b", false) && !valid_property_name("", false));
    }
}
