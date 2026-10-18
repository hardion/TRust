//! Tango data model: RFC-9 data types, DevFailed and the metadata of commands, attributes and pipes.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PROTOCOL_VERSION: u32 = 1;

pub type DevResult<T> = Result<T, DevFailed>;

/// Seconds since the Unix epoch.
pub fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DevState {
    On,
    Off,
    Close,
    Open,
    Insert,
    Extract,
    Moving,
    Standby,
    Fault,
    Init,
    Running,
    Alarm,
    Disable,
    #[default]
    Unknown,
}

impl fmt::Display for DevState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{self:?}").to_uppercase())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevEncoded {
    pub format: String,
    pub data: Vec<u8>,
}

/// Generates `Value` (RFC-9 value) and `DataType` (its tag) from the scalar/array type pairs.
macro_rules! data_types {
    (numeric: $($n:ident($nt:ty) $na:ident),*; other: $($o:ident($ot:ty) $oa:ident),*) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        pub enum Value {
            Void,
            $($n($nt), $na(Vec<$nt>),)*
            $($o($ot), $oa(Vec<$ot>),)*
            LongStringArray(Vec<i32>, Vec<String>),
            DoubleStringArray(Vec<f64>, Vec<String>),
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
        pub enum DataType {
            #[default]
            Void,
            $($n, $na,)*
            $($o, $oa,)*
            LongStringArray,
            DoubleStringArray,
        }

        impl Value {
            pub fn data_type(&self) -> DataType {
                match self {
                    Value::Void => DataType::Void,
                    $(Value::$n(_) => DataType::$n, Value::$na(_) => DataType::$na,)*
                    $(Value::$o(_) => DataType::$o, Value::$oa(_) => DataType::$oa,)*
                    Value::LongStringArray(..) => DataType::LongStringArray,
                    Value::DoubleStringArray(..) => DataType::DoubleStringArray,
                }
            }

            /// Number of elements: 0 for Void, 1 for a scalar.
            #[allow(clippy::len_without_is_empty)]
            pub fn len(&self) -> usize {
                match self {
                    Value::Void => 0,
                    $(Value::$na(v) => v.len(),)*
                    $(Value::$oa(v) => v.len(),)*
                    _ => 1,
                }
            }

            /// Numeric elements as f64, empty for non numeric types.
            pub fn numbers(&self) -> Vec<f64> {
                match self {
                    $(Value::$n(v) => vec![*v as f64], Value::$na(v) => v.iter().map(|x| *x as f64).collect(),)*
                    _ => vec![],
                }
            }

            /// True if a numeric element is outside `[lo, hi]`.
            pub fn outside(&self, lo: Option<f64>, hi: Option<f64>) -> bool {
                self.numbers().iter().any(|x| lo.is_some_and(|l| *x < l) || hi.is_some_and(|h| *x > h))
            }
        }

        impl DataType {
            /// The array type carrying SPECTRUM and IMAGE values of this scalar type.
            pub fn array(self) -> Self {
                match self {
                    $(DataType::$n => DataType::$na,)*
                    $(DataType::$o => DataType::$oa,)*
                    t => t,
                }
            }
        }
    };
}

data_types! {
    numeric: UChar(u8) CharArray, Short(i16) ShortArray, UShort(u16) UShortArray, Long(i32) LongArray,
        ULong(u32) ULongArray, Long64(i64) Long64Array, ULong64(u64) ULong64Array, Float(f32) FloatArray,
        Double(f64) DoubleArray;
    other: Boolean(bool) BooleanArray, String(String) StringArray, State(DevState) StateArray,
        Encoded(DevEncoded) EncodedArray
}

// ------------------------------------------------------------------ errors

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ErrSeverity {
    Warn,
    #[default]
    Err,
    Panic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevError {
    pub reason: String,
    pub desc: String,
    pub origin: String,
    pub severity: ErrSeverity,
}

/// The Tango exception: a non-empty stack of errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevFailed(pub Vec<DevError>);

impl DevFailed {
    pub fn new(reason: impl Into<String>, desc: impl Into<String>, origin: impl Into<String>) -> Self {
        let (reason, desc, origin) = (reason.into(), desc.into(), origin.into());
        Self(vec![DevError { reason, desc, origin, severity: ErrSeverity::Err }])
    }

    pub fn reason(&self) -> &str {
        self.0.first().map_or("", |e| &e.reason)
    }
}

impl fmt::Display for DevFailed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for e in &self.0 {
            writeln!(f, "{}: {} ({})", e.reason, e.desc, e.origin)?;
        }
        Ok(())
    }
}

impl std::error::Error for DevFailed {}

impl From<std::io::Error> for DevFailed {
    fn from(e: std::io::Error) -> Self {
        Self::new("API_CommunicationFailed", e.to_string(), "socket")
    }
}

impl From<serde_json::Error> for DevFailed {
    fn from(e: serde_json::Error) -> Self {
        Self::new("API_InvalidRequest", e.to_string(), "frame")
    }
}

// ------------------------------------------------------------------ metadata

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DispLevel {
    #[default]
    Operator,
    Expert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFormat {
    #[default]
    Scalar,
    Spectrum,
    Image,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Writable {
    #[default]
    Read,
    Write,
    ReadWrite,
    ReadWithWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AttrQuality {
    #[default]
    Valid,
    Invalid,
    Alarm,
    Changing,
    Warning,
}

/// Tango DevSource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Source {
    #[default]
    Dev,
    Cache,
    CacheDev,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CmdInfo {
    pub name: String,
    pub in_type: DataType,
    pub out_type: DataType,
    pub in_desc: String,
    pub out_desc: String,
    pub disp_level: DispLevel,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AttrInfo {
    pub name: String,
    pub data_type: DataType,
    pub data_format: DataFormat,
    pub writable: Writable,
    pub max_dim_x: usize,
    pub max_dim_y: usize,
    pub disp_level: DispLevel,
    pub description: String,
    pub label: String,
    pub unit: String,
    pub format: String,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub min_alarm: Option<f64>,
    pub max_alarm: Option<f64>,
    pub min_warning: Option<f64>,
    pub max_warning: Option<f64>,
}

impl AttrInfo {
    /// Builds the read reply for a value of this attribute (dimensions and quality).
    pub fn value(&self, value: Value, w_value: Option<Value>) -> AttrValue {
        let (dim_x, dim_y) = match self.data_format {
            DataFormat::Scalar => (1, 0),
            DataFormat::Spectrum => (value.len(), 0),
            DataFormat::Image => (self.max_dim_x, value.len() / self.max_dim_x.max(1)),
        };
        let quality = if value.outside(self.min_alarm, self.max_alarm) {
            AttrQuality::Alarm
        } else if value.outside(self.min_warning, self.max_warning) {
            AttrQuality::Warning
        } else {
            AttrQuality::Valid
        };
        AttrValue { name: self.name.clone(), value, w_value, quality, time: now(), dim_x, dim_y }
    }

    pub fn has_alarms(&self) -> bool {
        [self.min_alarm, self.max_alarm, self.min_warning, self.max_warning].iter().any(Option::is_some)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttrValue {
    pub name: String,
    pub value: Value,
    pub w_value: Option<Value>,
    pub quality: AttrQuality,
    pub time: f64,
    pub dim_x: usize,
    pub dim_y: usize,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PipeInfo {
    pub name: String,
    pub description: String,
    pub label: String,
    pub disp_level: DispLevel,
    pub writable: bool,
}

/// DevPipeBlob: named list of key/value elements.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PipeBlob {
    pub name: String,
    pub elements: Vec<(String, Value)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipeValue {
    pub name: String,
    pub time: f64,
    pub blob: PipeBlob,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub dev_class: String,
    pub server_id: String,
    pub server_host: String,
    pub server_version: u32,
    pub doc_url: String,
    pub dev_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlackBoxRecord {
    pub time: f64,
    pub operation: String,
    pub source: Source,
    pub client_id: u64,
    pub host: String,
}

/// One entry of a polling ring buffer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct History<T> {
    pub time: f64,
    pub value: DevResult<T>,
}

pub type CmdHistory = History<Value>;
pub type AttrHistory = History<AttrValue>;
