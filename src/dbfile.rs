//! Database as a file (RFC-6): the read-only resource file syntax.

use crate::types::{DevFailed, DevResult};
use std::collections::BTreeMap;

/// The content of a Database file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DbFile {
    /// `(server/instance, class, device)`
    pub devices: Vec<(String, String, String)>,
    /// `(device, property) -> values`
    pub device_props: BTreeMap<(String, String), Vec<String>>,
    /// `(device, attribute, property) -> values`
    pub attr_props: BTreeMap<(String, String, String), Vec<String>>,
    /// `(class, property) -> values`
    pub class_props: BTreeMap<(String, String), Vec<String>>,
}

/// `a, "b c", d` -> `["a", "b c", "d"]`
fn values(v: &str) -> Vec<String> {
    let (mut out, mut cur, mut quoted) = (vec![], String::new(), false);
    for c in v.chars() {
        match c {
            '"' => quoted = !quoted,
            ',' if !quoted => out.push(std::mem::take(&mut cur).trim().to_string()),
            c => cur.push(c),
        }
    }
    out.push(cur.trim().to_string());
    out
}

impl DbFile {
    pub fn read(path: &std::path::Path) -> DevResult<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    pub fn parse(text: &str) -> DevResult<Self> {
        let mut f = Self::default();
        let mut line = String::new();
        for raw in text.lines() {
            let raw = raw.trim();
            if line.is_empty() && (raw.is_empty() || raw.starts_with('#')) {
                continue;
            }
            if let Some(cont) = raw.strip_suffix('\\') {
                line.push_str(cont);
                continue;
            }
            line.push_str(raw);
            f.statement(&std::mem::take(&mut line))?;
        }
        Ok(f)
    }

    fn statement(&mut self, line: &str) -> DevResult<()> {
        let bad = || DevFailed::new("API_DatabaseFileError", format!("cannot parse `{line}`"), "DbFile");
        let (key, value) = line.split_once(':').ok_or_else(bad)?;
        let (key, vals) = (key.trim(), values(value));
        if let Some((target, prop)) = key.split_once("->") {
            let parts: Vec<&str> = target.split('/').collect();
            let prop = prop.to_string();
            match parts[..] {
                ["CLASS", class] => drop(self.class_props.insert((class.into(), prop), vals)),
                [_, _, _] => drop(self.device_props.insert((target.into(), prop), vals)),
                [d, f, m, attr] => drop(self.attr_props.insert((format!("{d}/{f}/{m}"), attr.into(), prop), vals)),
                _ => return Err(bad()),
            }
        } else {
            let parts: Vec<&str> = key.split('/').collect();
            let [server, instance, "DEVICE", class] = parts[..] else { return Err(bad()) };
            for dev in vals.into_iter().filter(|d| !d.is_empty()) {
                self.devices.push((format!("{server}/{instance}"), class.into(), dev));
            }
        }
        Ok(())
    }
}
