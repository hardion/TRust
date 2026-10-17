//! Client side: `DeviceProxy`, one connection to one device.

use crate::grpc::Link;
use crate::protocol::*;
use crate::types::*;
use std::time::{Duration, Instant};

pub struct DeviceProxy {
    device: String,
    source: Source,
    next_id: u64,
    link: Link,
}

pub(crate) fn bad_reply(what: impl std::fmt::Debug) -> DevFailed {
    DevFailed::new("API_CommunicationFailed", format!("unexpected reply {what:?}"), "DeviceProxy")
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

fn pairs(values: &[(&str, Value)]) -> Vec<(String, Value)> {
    values.iter().map(|(n, v)| (n.to_string(), v.clone())).collect()
}

/// Splits `<endpoint>/domain/family/member`: the device name is always the last three segments.
pub(crate) fn split_url(url: &str) -> DevResult<(&str, &str)> {
    let cut = url.char_indices().filter(|(_, c)| *c == '/').map(|(i, _)| i).rev().nth(2);
    match cut {
        Some(i) if i > 0 => Ok((&url[..i], &url[i + 1..])),
        _ => Err(DevFailed::new(
            "API_InvalidArgs",
            format!("expected <endpoint>/domain/family/member, got {url}"),
            "url",
        )),
    }
}

/// Sends `$op` and extracts the `Data::$variant` payload (`()` for `Data::Void`).
macro_rules! call {
    ($self:ident, $op:expr) => {
        match $self.call($op).await? {
            Data::Void => Ok(()),
            other => Err(bad_reply(other)),
        }
    };
    ($self:ident, $op:expr, $variant:ident) => {
        match $self.call($op).await? {
            Data::$variant(x) => Ok(x),
            other => Err(bad_reply(other)),
        }
    };
}

impl DeviceProxy {
    /// `url` is `<endpoint>/domain/family/member`, here `grpc://host:port/domain/family/member`.
    pub async fn connect(url: &str) -> DevResult<Self> {
        let (endpoint, device) = split_url(url)?;
        let endpoint = endpoint.strip_prefix("grpc://").unwrap_or(endpoint);
        Ok(Self { device: device.into(), source: Source::Dev, next_id: 0, link: Link::connect(endpoint).await? })
    }

    pub fn set_source(&mut self, source: Source) {
        self.source = source;
    }

    async fn call(&mut self, op: Op) -> DevResult<Data> {
        self.next_id += 1;
        let (id, client, source, device) = (self.next_id, std::process::id() as u64, self.source, self.device.clone());
        let reply = self.link.call(Request { v: PROTOCOL_VERSION, id, client, source, device, op }).await?;
        if reply.id != id {
            return Err(bad_reply(reply));
        }
        reply.reply
    }

    pub async fn name(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceName, String)
    }

    pub async fn description(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceDescription, String)
    }

    pub async fn adm_name(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceAdmName, String)
    }

    /// Round trip time.
    pub async fn ping(&mut self) -> DevResult<Duration> {
        let start = Instant::now();
        call!(self, Op::DevicePing)?;
        Ok(start.elapsed())
    }

    pub async fn black_box(&mut self, n: usize) -> DevResult<Vec<BlackBoxRecord>> {
        call!(self, Op::DeviceBlackbox { n }, BlackBox)
    }

    pub async fn info(&mut self) -> DevResult<DeviceInfo> {
        call!(self, Op::DeviceInfo, Info)
    }

    pub async fn command_inout(&mut self, name: &str, arg: Value) -> DevResult<Value> {
        call!(self, Op::CommandExecution { name: name.into(), arg }, Value)
    }

    pub async fn command_list_query(&mut self) -> DevResult<Vec<CmdInfo>> {
        call!(self, Op::CommandsList, Commands)
    }

    pub async fn command_query(&mut self, name: &str) -> DevResult<CmdInfo> {
        let mut infos = call!(self, Op::CommandInfo { name: name.into() }, Commands)?;
        infos.pop().ok_or_else(|| bad_reply("no command info"))
    }

    pub async fn command_history(&mut self, name: &str, n: usize) -> DevResult<Vec<CmdHistory>> {
        call!(self, Op::CommandHistory { name: name.into(), n }, CmdHistory)
    }

    pub async fn read_attributes(&mut self, names: &[&str]) -> DevResult<Vec<DevResult<AttrValue>>> {
        call!(self, Op::AttributesRead { names: strings(names) }, Attributes)
    }

    pub async fn read_attribute(&mut self, name: &str) -> DevResult<AttrValue> {
        self.read_attributes(&[name]).await?.pop().ok_or_else(|| bad_reply("no attribute value"))?
    }

    pub async fn write_attributes(&mut self, values: &[(&str, Value)]) -> DevResult<()> {
        call!(self, Op::AttributesWrite { values: pairs(values) })
    }

    pub async fn write_attribute(&mut self, name: &str, value: Value) -> DevResult<()> {
        self.write_attributes(&[(name, value)]).await
    }

    pub async fn write_read_attributes(
        &mut self,
        values: &[(&str, Value)],
        names: &[&str],
    ) -> DevResult<Vec<DevResult<AttrValue>>> {
        call!(self, Op::AttributesWriteRead { values: pairs(values), names: strings(names) }, Attributes)
    }

    /// Empty `names` returns every attribute.
    pub async fn get_attribute_config(&mut self, names: &[&str]) -> DevResult<Vec<AttrInfo>> {
        call!(self, Op::AttributeGetConfig { names: strings(names) }, AttrConfigs)
    }

    pub async fn set_attribute_config(&mut self, configs: Vec<AttrInfo>) -> DevResult<Vec<AttrInfo>> {
        call!(self, Op::AttributeSetConfig { configs }, AttrConfigs)
    }

    pub async fn attribute_history(&mut self, name: &str, n: usize) -> DevResult<Vec<AttrHistory>> {
        call!(self, Op::AttributeHistory { name: name.into(), n }, AttrHistory)
    }

    pub async fn read_pipe(&mut self, name: &str) -> DevResult<PipeValue> {
        call!(self, Op::PipeRead { name: name.into() }, Pipe)
    }

    pub async fn write_pipe(&mut self, name: &str, blob: PipeBlob) -> DevResult<()> {
        call!(self, Op::PipeWrite { name: name.into(), blob })
    }

    pub async fn write_read_pipe(&mut self, name: &str, blob: PipeBlob) -> DevResult<PipeValue> {
        call!(self, Op::PipeWriteRead { name: name.into(), blob }, Pipe)
    }

    /// Empty `names` returns every pipe.
    pub async fn get_pipe_config(&mut self, names: &[&str]) -> DevResult<Vec<PipeInfo>> {
        call!(self, Op::PipeGetConfig { names: strings(names) }, PipeConfigs)
    }

    pub async fn set_pipe_config(&mut self, configs: Vec<PipeInfo>) -> DevResult<Vec<PipeInfo>> {
        call!(self, Op::PipeSetConfig { configs }, PipeConfigs)
    }
}
