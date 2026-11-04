//! Client of the Database device `sys/database/2` (RFC-6), with the C++ DataBaseds argument layouts.

use crate::client::DeviceProxy;
use crate::types::{DevFailed, DevResult, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const DATABASE_DEVICE: &str = "sys/database/2";

/// Endpoint URL of the Database at `tango_host` (`host:port`, or a URL with a transport scheme).
pub(crate) fn database_endpoint(tango_host: &str) -> String {
    match tango_host.contains("://") {
        true => tango_host.to_string(),
        false if cfg!(feature = "detect") => format!("auto://{tango_host}"),
        false => format!("tcp://{tango_host}"),
    }
}

/// `$TANGO_HOST`, if set.
pub(crate) fn tango_host() -> Option<String> {
    std::env::var("TANGO_HOST").ok().filter(|h| !h.is_empty())
}

/// This host's name (`$HOSTNAME`, `/etc/hostname`, else `localhost`).
pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".into())
}

fn sa(v: impl IntoIterator<Item = impl Into<String>>) -> Value {
    Value::StringArray(v.into_iter().map(Into::into).collect())
}

fn strings(v: Value) -> DevResult<Vec<String>> {
    match v {
        Value::StringArray(v) => Ok(v),
        other => Err(DevFailed::new("API_IncompatibleArgumentType", format!("{other:?}"), "Database")),
    }
}

/// `[name, count, values...]*` from `list[at..]`, `count` entries; returns them and the next index.
fn unpack(list: &[String], mut at: usize, count: usize) -> (HashMap<String, Vec<String>>, usize) {
    let mut out = HashMap::new();
    for _ in 0..count {
        let (Some(name), Some(n)) = (list.get(at), list.get(at + 1).and_then(|n| n.parse::<usize>().ok())) else {
            break;
        };
        if n > 0 && at + 2 + n <= list.len() {
            out.insert(name.to_lowercase(), list[at + 2..at + 2 + n].to_vec());
        }
        at += 2 + n;
    }
    (out, at)
}

fn count(list: &[String], at: usize) -> usize {
    list.get(at).and_then(|n| n.parse().ok()).unwrap_or(0)
}

/// A connection to the Database device; cheap to clone.
#[derive(Clone)]
pub struct Database(Arc<Mutex<DeviceProxy>>);

impl Database {
    /// `tango_host` is `host:port` (transport detected) or a URL such as `grpc://host:10000`.
    // a web page has one thread: the proxy (a browser WebSocket) needs not be Send
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub async fn connect(tango_host: &str) -> DevResult<Self> {
        let proxy = DeviceProxy::direct(&database_endpoint(tango_host), DATABASE_DEVICE).await?;
        Ok(Self(Arc::new(Mutex::new(proxy))))
    }

    /// Executes any Database command.
    pub async fn command(&self, name: &str, arg: Value) -> DevResult<Value> {
        self.0.lock().await.command_inout(name, arg).await
    }

    async fn list(&self, name: &str, arg: Value) -> DevResult<Vec<String>> {
        strings(self.command(name, arg).await?)
    }

    /// `(exported, endpoint URL)` of a device.
    pub async fn import(&self, device: &str) -> DevResult<(bool, String)> {
        match self.command("DbImportDevice", Value::String(device.into())).await? {
            Value::LongStringArray(l, s) if !l.is_empty() && s.len() > 1 => Ok((l[0] == 1, s[1].clone())),
            other => Err(DevFailed::new("API_CommunicationFailed", format!("{other:?}"), "DbImportDevice")),
        }
    }

    pub async fn alias_device(&self, alias: &str) -> DevResult<String> {
        match self.command("DbGetAliasDevice", Value::String(alias.into())).await? {
            Value::String(d) => Ok(d),
            other => Err(DevFailed::new("API_CommunicationFailed", format!("{other:?}"), "DbGetAliasDevice")),
        }
    }

    pub async fn add_device(&self, server: &str, device: &str, class: &str) -> DevResult<()> {
        self.command("DbAddDevice", sa([server, device, class])).await.map(drop)
    }

    pub async fn export(&self, device: &str, endpoint: &str, host: &str) -> DevResult<()> {
        let arg = sa([device, endpoint, host, &std::process::id().to_string(), "1"]);
        self.command("DbExportDevice", arg).await.map(drop)
    }

    pub async fn unexport_server(&self, server: &str) -> DevResult<()> {
        self.command("DbUnExportServer", Value::String(server.into())).await.map(drop)
    }

    /// `(device, class)` of a server, its admin device excluded.
    pub async fn server_devices(&self, server: &str) -> DevResult<Vec<(String, String)>> {
        let l = self.list("DbGetDeviceClassList", Value::String(server.into())).await?;
        Ok(l.chunks(2).filter(|c| c.len() == 2 && c[1] != "DServer").map(|c| (c[0].clone(), c[1].clone())).collect())
    }

    /// The device (or class, with `class = true`) properties among `names` that have a value.
    pub async fn properties(
        &self,
        owner: &str,
        names: &[String],
        class: bool,
    ) -> DevResult<HashMap<String, Vec<String>>> {
        let cmd = if class { "DbGetClassProperty" } else { "DbGetDeviceProperty" };
        let l = self.list(cmd, sa(std::iter::once(owner.to_string()).chain(names.iter().cloned()))).await?;
        Ok(unpack(&l, 2, count(&l, 1)).0)
    }

    /// `attribute -> property -> values` for the attributes of a device.
    pub async fn attribute_properties(
        &self,
        device: &str,
        attrs: &[String],
    ) -> DevResult<HashMap<String, HashMap<String, Vec<String>>>> {
        let arg = sa(std::iter::once(device.to_string()).chain(attrs.iter().cloned()));
        let l = self.list("DbGetDeviceAttributeProperty2", arg).await?;
        let (mut out, mut at) = (HashMap::new(), 2);
        for _ in 0..count(&l, 1) {
            let Some(attr) = l.get(at) else { break };
            let (props, next) = unpack(&l, at + 2, count(&l, at + 1));
            out.insert(attr.to_lowercase(), props);
            at = next;
        }
        Ok(out)
    }

    /// Writes (`Some`) or deletes (`None`) attribute properties of a device.
    pub async fn put_attribute_properties(
        &self,
        device: &str,
        attr: &str,
        props: &[(String, Option<Vec<String>>)],
    ) -> DevResult<()> {
        let (put, del): (Vec<_>, Vec<_>) = props.iter().partition(|(_, v)| v.is_some());
        if !put.is_empty() {
            let mut arg = vec![device.to_string(), "1".into(), attr.into(), put.len().to_string()];
            for (name, v) in put {
                let v = v.as_ref().expect("partitioned");
                arg.extend([name.clone(), v.len().to_string()]);
                arg.extend(v.iter().cloned());
            }
            self.command("DbPutDeviceAttributeProperty2", sa(arg)).await?;
        }
        if !del.is_empty() {
            let arg = [device.to_string(), attr.into()].into_iter().chain(del.iter().map(|(n, _)| n.clone()));
            self.command("DbDeleteDeviceAttributeProperty", sa(arg)).await?;
        }
        Ok(())
    }
}
