//! Request/Reply frames: one JSON object per line (see docs/SPEC.md).

use crate::types::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub v: u32,
    pub id: u64,
    #[serde(default)]
    pub client: u64,
    #[serde(default)]
    pub source: Source,
    pub device: String,
    pub op: Op,
}

/// The request types of the Tango Request-Reply cheatsheet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    DeviceName,
    DeviceDescription,
    DeviceAdmName,
    DevicePing,
    DeviceBlackbox { n: usize },
    DeviceInfo,
    CommandExecution { name: String, arg: Value },
    CommandsList,
    CommandInfo { name: String },
    CommandHistory { name: String, n: usize },
    AttributesRead { names: Vec<String> },
    AttributesWrite { values: Vec<(String, Value)> },
    AttributesWriteRead { values: Vec<(String, Value)>, names: Vec<String> },
    AttributeSetConfig { configs: Vec<AttrInfo> },
    AttributeGetConfig { names: Vec<String> },
    AttributeHistory { name: String, n: usize },
    PipeRead { name: String },
    PipeWrite { name: String, blob: PipeBlob },
    PipeWriteRead { name: String, blob: PipeBlob },
    PipeGetConfig { names: Vec<String> },
    PipeSetConfig { configs: Vec<PipeInfo> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub id: u64,
    pub reply: DevResult<Data>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Data {
    Void,
    String(String),
    Info(DeviceInfo),
    BlackBox(Vec<BlackBoxRecord>),
    Value(Value),
    Commands(Vec<CmdInfo>),
    CmdHistory(Vec<CmdHistory>),
    Attributes(Vec<DevResult<AttrValue>>),
    AttrConfigs(Vec<AttrInfo>),
    AttrHistory(Vec<AttrHistory>),
    Pipe(PipeValue),
    PipeConfigs(Vec<PipeInfo>),
}
