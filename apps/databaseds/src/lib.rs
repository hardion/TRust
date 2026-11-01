//! The Tango Database device `sys/database/2` (RFC-6) on SQLite, with the argument layouts of the
//! C++ DataBaseds (docs/SPEC.md §7.2). Properties of every kind share one table keyed by `kind`.

use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value as Sql};
use std::path::Path;
use trust::{CmdInfo, DataType as T, DevFailed, DevResult, DevState, Device, Server, Value, valid_property_name};

pub const DEVICE: &str = "sys/database/2";
pub const SERVER: &str = "DataBaseds/2";
const HISTORY_DEPTH: usize = 10;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS device(
    name TEXT PRIMARY KEY COLLATE NOCASE, alias TEXT UNIQUE COLLATE NOCASE,
    domain TEXT COLLATE NOCASE, family TEXT COLLATE NOCASE, member TEXT COLLATE NOCASE,
    server TEXT COLLATE NOCASE, class TEXT COLLATE NOCASE, exported INTEGER DEFAULT 0,
    ior TEXT DEFAULT '', host TEXT DEFAULT '', pid INTEGER DEFAULT 0, version TEXT DEFAULT '',
    started TEXT DEFAULT '', stopped TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS server(name TEXT PRIMARY KEY COLLATE NOCASE, host TEXT COLLATE NOCASE,
    mode INTEGER DEFAULT 0, level INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS property(kind TEXT, object TEXT COLLATE NOCASE, sub TEXT COLLATE NOCASE,
    name TEXT COLLATE NOCASE, idx INTEGER, value TEXT, PRIMARY KEY(kind, object, sub, name, idx));
CREATE TABLE IF NOT EXISTS property_hist(id INTEGER, kind TEXT, object TEXT COLLATE NOCASE,
    sub TEXT COLLATE NOCASE, name TEXT COLLATE NOCASE, date TEXT, idx INTEGER, value TEXT);
CREATE TABLE IF NOT EXISTS attribute_alias(alias TEXT PRIMARY KEY COLLATE NOCASE, name TEXT COLLATE NOCASE);
CREATE TABLE IF NOT EXISTS event(name TEXT PRIMARY KEY COLLATE NOCASE, exported INTEGER, ior TEXT,
    host TEXT, pid INTEGER, version TEXT);
";

/// The commands of RFC-6 with their Tango argument types.
#[rustfmt::skip]
const COMMANDS: &[(&str, T, T)] = &[
    ("DbAddDevice", T::StringArray, T::Void), ("DbAddServer", T::StringArray, T::Void),
    ("DbDeleteDevice", T::String, T::Void), ("DbDeleteServer", T::String, T::Void),
    ("DbRenameServer", T::StringArray, T::Void), ("DbGetServerList", T::String, T::StringArray),
    ("DbGetServerNameList", T::String, T::StringArray), ("DbGetInstanceNameList", T::String, T::StringArray),
    ("DbGetDeviceList", T::StringArray, T::StringArray), ("DbGetDeviceClassList", T::String, T::StringArray),
    ("DbGetDeviceServerClassList", T::String, T::StringArray), ("DbGetClassList", T::String, T::StringArray),
    ("DbGetClassForDevice", T::String, T::String), ("DbGetClassInheritanceForDevice", T::String, T::StringArray),
    ("DbGetDeviceDomainList", T::String, T::StringArray), ("DbGetDeviceFamilyList", T::String, T::StringArray),
    ("DbGetDeviceMemberList", T::String, T::StringArray), ("DbGetDeviceWideList", T::String, T::StringArray),
    ("DbGetDeviceInfo", T::String, T::LongStringArray),
    ("DbPutDeviceProperty", T::StringArray, T::Void), ("DbGetDeviceProperty", T::StringArray, T::StringArray),
    ("DbDeleteDeviceProperty", T::StringArray, T::Void), ("DbGetDevicePropertyList", T::StringArray, T::StringArray),
    ("DbGetDevicePropertyHist", T::StringArray, T::StringArray),
    ("DbPutClassProperty", T::StringArray, T::Void), ("DbGetClassProperty", T::StringArray, T::StringArray),
    ("DbDeleteClassProperty", T::StringArray, T::Void), ("DbGetClassPropertyList", T::String, T::StringArray),
    ("DbGetClassPropertyHist", T::StringArray, T::StringArray),
    ("DbPutProperty", T::StringArray, T::Void), ("DbGetProperty", T::StringArray, T::StringArray),
    ("DbDeleteProperty", T::StringArray, T::Void), ("DbGetPropertyList", T::StringArray, T::StringArray),
    ("DbGetPropertyHist", T::StringArray, T::StringArray), ("DbGetObjectList", T::String, T::StringArray),
    ("DbPutDeviceAttributeProperty", T::StringArray, T::Void), ("DbPutDeviceAttributeProperty2", T::StringArray, T::Void),
    ("DbGetDeviceAttributeProperty", T::StringArray, T::StringArray), ("DbGetDeviceAttributeProperty2", T::StringArray, T::StringArray),
    ("DbDeleteDeviceAttributeProperty", T::StringArray, T::Void), ("DbDeleteAllDeviceAttributeProperty", T::StringArray, T::Void),
    ("DbGetDeviceAttributePropertyHist", T::StringArray, T::StringArray), ("DbGetDeviceAttributeList", T::StringArray, T::StringArray),
    ("DbDeleteDeviceAttribute", T::StringArray, T::Void),
    ("DbPutClassAttributeProperty", T::StringArray, T::Void), ("DbPutClassAttributeProperty2", T::StringArray, T::Void),
    ("DbGetClassAttributeProperty", T::StringArray, T::StringArray), ("DbGetClassAttributeProperty2", T::StringArray, T::StringArray),
    ("DbDeleteClassAttributeProperty", T::StringArray, T::Void), ("DbGetClassAttributePropertyHist", T::StringArray, T::StringArray),
    ("DbGetClassAttributeList", T::StringArray, T::StringArray), ("DbDeleteClassAttribute", T::StringArray, T::Void),
    ("DbPutDevicePipeProperty", T::StringArray, T::Void), ("DbGetDevicePipeProperty", T::StringArray, T::StringArray),
    ("DbDeleteDevicePipeProperty", T::StringArray, T::Void), ("DbDeleteAllDevicePipeProperty", T::StringArray, T::Void),
    ("DbGetDevicePipePropertyHist", T::StringArray, T::StringArray), ("DbGetDevicePipeList", T::StringArray, T::StringArray),
    ("DbDeleteDevicePipe", T::StringArray, T::Void),
    ("DbPutClassPipeProperty", T::StringArray, T::Void), ("DbGetClassPipeProperty", T::StringArray, T::StringArray),
    ("DbDeleteClassPipeProperty", T::StringArray, T::Void), ("DbGetClassPipePropertyHist", T::StringArray, T::StringArray),
    ("DbGetClassPipeList", T::StringArray, T::StringArray), ("DbDeleteClassPipe", T::StringArray, T::Void),
    ("DbPutDeviceAlias", T::StringArray, T::Void), ("DbGetDeviceAlias", T::String, T::String),
    ("DbGetAliasDevice", T::String, T::String), ("DbDeleteDeviceAlias", T::String, T::Void),
    ("DbGetDeviceAliasList", T::String, T::StringArray), ("DbPutAttributeAlias", T::StringArray, T::Void),
    ("DbGetAttributeAlias", T::String, T::String), ("DbGetAliasAttribute", T::String, T::String),
    ("DbGetAttributeAlias2", T::String, T::String), ("DbDeleteAttributeAlias", T::String, T::Void),
    ("DbGetAttributeAliasList", T::String, T::StringArray),
    ("DbGetHostList", T::String, T::StringArray), ("DbGetHostServerList", T::String, T::StringArray),
    ("DbGetHostServersInfo", T::String, T::StringArray), ("DbPutServerInfo", T::StringArray, T::Void),
    ("DbGetServerInfo", T::String, T::StringArray), ("DbDeleteServerInfo", T::String, T::Void),
    ("DbExportDevice", T::StringArray, T::Void), ("DbUnExportDevice", T::String, T::Void),
    ("DbUnExportServer", T::String, T::Void), ("DbImportDevice", T::String, T::LongStringArray),
    ("DbGetDeviceExportedList", T::String, T::StringArray), ("DbGetExportedDeviceListForClass", T::String, T::StringArray),
    ("DbExportEvent", T::StringArray, T::Void), ("DbImportEvent", T::String, T::LongStringArray),
    ("DbUnExportEvent", T::String, T::Void),
    ("DbInfo", T::Void, T::StringArray), ("DbGetDataForServerCache", T::StringArray, T::StringArray),
    ("DbGetForwardedAttributeListForDevice", T::String, T::StringArray), ("DbGetCSDbServerList", T::Void, T::StringArray),
    ("ResetTimingValues", T::Void, T::Void), ("DbMySqlSelect", T::String, T::LongStringArray),
];

fn fail(reason: &str, desc: impl Into<String>) -> DevFailed {
    DevFailed::new(reason, desc, "DataBase")
}

fn sql(e: rusqlite::Error) -> DevFailed {
    fail("DB_SQLError", e.to_string())
}

fn like(wildcard: &str) -> String {
    wildcard.replace('*', "%")
}

fn sa(v: Vec<String>) -> Value {
    Value::StringArray(v)
}

fn at(a: &[String], i: usize) -> DevResult<&str> {
    a.get(i).map(String::as_str).ok_or_else(|| fail("DB_IncorrectArguments", format!("argument {i} missing")))
}

fn num(a: &[String], i: usize) -> DevResult<usize> {
    at(a, i)?.trim().parse().map_err(|_| fail("DB_IncorrectArguments", format!("argument {i} is not a count")))
}

fn check_device(name: &str) -> DevResult<()> {
    let parts: Vec<&str> = name.split('/').collect();
    let ok = parts.len() == 3 && name.len() <= 255 && parts.iter().all(|p| !p.is_empty() && p.len() <= 85);
    if ok { Ok(()) } else { Err(fail("DB_IncorrectDeviceName", format!("{name} is not domain/family/member"))) }
}

fn check_server(name: &str) -> DevResult<()> {
    match name.split_once('/') {
        Some((e, i)) if !e.is_empty() && !i.is_empty() && !i.contains('/') => Ok(()),
        _ => Err(fail("DB_IncorrectServerName", format!("{name} is not executable/instance"))),
    }
}

/// The property kinds, and for attribute-like kinds the commands that name them.
#[derive(Clone, Copy)]
enum Kind {
    Device,
    Class,
    Free,
    DeviceAttr,
    ClassAttr,
    DevicePipe,
    ClassPipe,
}

impl Kind {
    fn key(self) -> &'static str {
        match self {
            Kind::Device => "device",
            Kind::Class => "class",
            Kind::Free => "free",
            Kind::DeviceAttr => "device_attr",
            Kind::ClassAttr => "class_attr",
            Kind::DevicePipe => "device_pipe",
            Kind::ClassPipe => "class_pipe",
        }
    }
}

/// The Database device.
pub struct DataBase {
    conn: Connection,
    endpoint: String,
}

/// The DataBaseds server for `path`, exporting itself with `endpoint` (e.g. `auto://host:10000`).
pub fn server(path: &Path, endpoint: &str) -> DevResult<Server> {
    Ok(Server::new("DataBaseds", "2").nodb().device(DEVICE, DataBase::open(path, endpoint)?))
}

impl DataBase {
    /// Opens (or creates) the SQLite file and registers the Database itself, exported at `endpoint`.
    pub fn open(path: &Path, endpoint: &str) -> DevResult<Self> {
        let conn = Connection::open(path).map_err(sql)?;
        conn.execute_batch(SCHEMA).map_err(sql)?;
        conn.busy_timeout(std::time::Duration::from_secs(5)).map_err(sql)?;
        let db = Self { conn, endpoint: endpoint.into() };
        let me = [SERVER, DEVICE, "DataBase"].map(String::from);
        db.add_server(&me)?;
        let pid = std::process::id().to_string();
        for dev in [DEVICE.to_string(), format!("dserver/{SERVER}")] {
            db.export(&[dev, endpoint.into(), trust::hostname(), pid.clone(), "1".into()])?;
        }
        Ok(db)
    }

    fn exec(&self, query: &str, p: impl rusqlite::Params) -> DevResult<usize> {
        self.conn.execute(query, p).map_err(sql)
    }

    /// The first column of every row.
    fn column(&self, query: &str, p: impl rusqlite::Params) -> DevResult<Vec<String>> {
        let mut stmt = self.conn.prepare(query).map_err(sql)?;
        let rows = stmt.query_map(p, |r| r.get::<_, String>(0)).map_err(sql)?;
        rows.collect::<Result<_, _>>().map_err(sql)
    }

    fn one(&self, query: &str, p: impl rusqlite::Params, missing: DevFailed) -> DevResult<String> {
        let v = self.conn.query_row(query, p, |r| r.get::<_, Option<String>>(0)).optional().map_err(sql)?;
        v.flatten().filter(|v| !v.is_empty()).ok_or(missing)
    }

    fn not_defined(device: &str) -> DevFailed {
        fail("DB_DeviceNotDefined", format!("device {device} not defined in the database"))
    }

    // ---------------------------------------------------------------- devices and servers

    fn add_device(&self, server: &str, device: &str, class: &str) -> DevResult<()> {
        check_server(server)?;
        check_device(device)?;
        let p: Vec<&str> = device.split('/').collect();
        self.exec(
            "INSERT INTO device(name, domain, family, member, server, class) VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(name) DO UPDATE SET server = ?5, class = ?6",
            params![device, p[0], p[1], p[2], server, class],
        )?;
        Ok(())
    }

    /// `[server, (device, class)*]`, the admin device included.
    fn add_server(&self, a: &[String]) -> DevResult<()> {
        let server = at(a, 0)?;
        self.add_device(server, &format!("dserver/{server}"), "DServer")?;
        for pair in a[1..].chunks(2) {
            self.add_device(server, at(pair, 0)?, at(pair, 1)?)?;
        }
        Ok(())
    }

    fn delete_device(&self, device: &str) -> DevResult<()> {
        self.exec("DELETE FROM device WHERE name = ?1", [device])?;
        self.exec(
            "DELETE FROM property WHERE object = ?1 AND kind IN ('device', 'device_attr', 'device_pipe')",
            [device],
        )?;
        Ok(())
    }

    fn device_info(&self, device: &str, full: bool) -> DevResult<Value> {
        let row = self
            .conn
            .query_row(
                "SELECT name, ior, version, server, host, started, stopped, class, exported, pid FROM device WHERE name = ?1",
                [device],
                |r| Ok(((0..8).map(|i| r.get::<_, String>(i)).collect::<Result<Vec<_>, _>>()?, r.get(8)?, r.get(9)?)),
            )
            .optional()
            .map_err(sql)?;
        let (mut s, exported, pid): (Vec<String>, i32, i32) = row.ok_or_else(|| Self::not_defined(device))?;
        if !full {
            // DbImportDevice: [name, ior, version, server, host, class]
            s = vec![s[0].clone(), s[1].clone(), s[2].clone(), s[3].clone(), s[4].clone(), s[7].clone()];
        }
        Ok(Value::LongStringArray(vec![exported, pid], s))
    }

    /// `[name, ior, host, pid, version]`
    fn export(&self, a: &[String]) -> DevResult<()> {
        let pid: i64 = at(a, 3)?.parse().unwrap_or(0);
        let n = self.exec(
            "UPDATE device SET exported = 1, ior = ?2, host = ?3, pid = ?4, version = ?5, started = datetime('now')
             WHERE name = ?1",
            params![at(a, 0)?, at(a, 1)?, at(a, 2)?, pid, at(a, 4)?],
        )?;
        if n == 0 { Err(Self::not_defined(at(a, 0)?)) } else { Ok(()) }
    }

    // ---------------------------------------------------------------- properties

    fn get_prop(&self, kind: Kind, obj: &str, sub: &str, name: &str) -> DevResult<Vec<String>> {
        self.column(
            "SELECT value FROM property WHERE kind = ?1 AND object = ?2 AND sub = ?3 AND name = ?4 ORDER BY idx",
            params![kind.key(), obj, sub, name],
        )
    }

    fn history_id(&self) -> DevResult<i64> {
        self.conn.query_row("SELECT COALESCE(MAX(id), 0) + 1 FROM property_hist", [], |r| r.get(0)).map_err(sql)
    }

    /// Writes (`Some`) or deletes (`None`) a property, and records it in the history.
    fn set_prop(&self, kind: Kind, obj: &str, sub: &str, name: &str, values: Option<&[String]>) -> DevResult<()> {
        let attribute = !matches!(kind, Kind::Device | Kind::Class | Kind::Free);
        if !valid_property_name(name, attribute) {
            return Err(fail("API_InvalidPropertyName", format!("{name} is not a valid property name (RFC-5)")));
        }
        let key = params![kind.key(), obj, sub, name];
        self.exec("DELETE FROM property WHERE kind = ?1 AND object = ?2 AND sub = ?3 AND name = ?4", key)?;
        let id = self.history_id()?;
        let hist = "INSERT INTO property_hist VALUES(?1, ?2, ?3, ?4, ?5, datetime('now'), ?6, ?7)";
        match values {
            Some(values) => {
                for (i, v) in values.iter().enumerate() {
                    self.exec(
                        "INSERT INTO property VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                        params![kind.key(), obj, sub, name, i, v],
                    )?;
                    self.exec(hist, params![id, kind.key(), obj, sub, name, i, v])?;
                }
            }
            None => drop(self.exec(hist, params![id, kind.key(), obj, sub, name, -1, Option::<String>::None])?),
        }
        Ok(())
    }

    fn names(&self, kind: Kind, obj: &str, sub: &str, wildcard: &str) -> DevResult<Vec<String>> {
        self.column(
            "SELECT DISTINCT name FROM property WHERE kind = ?1 AND object = ?2 AND sub = ?3 AND name LIKE ?4 ORDER BY name",
            params![kind.key(), obj, sub, like(wildcard)],
        )
    }

    /// `[(name, date, count, values...)*]`, oldest first, the last changes of each property.
    fn history(&self, kind: Kind, obj: &str, sub: &str, wildcard: &str, with_sub: bool) -> DevResult<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, name, date, idx, value FROM property_hist
                 WHERE kind = ?1 AND object = ?2 AND sub = ?3 AND name LIKE ?4 ORDER BY name, id, idx",
            )
            .map_err(sql)?;
        let rows = stmt
            .query_map(params![kind.key(), obj, sub, like(wildcard)], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(sql)?;
        // (id, name, date, values) per change
        let mut changes: Vec<(i64, String, String, Vec<String>)> = vec![];
        for row in rows {
            let (id, name, date, idx, value) = row.map_err(sql)?;
            if changes.last().is_none_or(|c| c.0 != id) {
                changes.push((id, name, date, vec![]));
            }
            if idx >= 0 {
                changes.last_mut().expect("pushed").3.extend(value);
            }
        }
        let mut out = vec![];
        for (i, (_, name, date, values)) in changes.iter().enumerate() {
            let later = changes[i..].iter().filter(|c| c.1.eq_ignore_ascii_case(name)).count();
            if later > HISTORY_DEPTH {
                continue;
            }
            if with_sub {
                out.push(sub.to_string());
            }
            out.extend([name.clone(), date.clone(), values.len().to_string()]);
            out.extend(values.iter().cloned());
        }
        Ok(out)
    }

    /// `[obj, n, (name, count, values...)*]` (put) for device, class and free properties.
    fn put_props(&self, kind: Kind, a: &[String]) -> DevResult<()> {
        let (obj, mut i) = (at(a, 0)?, 2);
        for _ in 0..num(a, 1)? {
            let (name, n) = (at(a, i)?, num(a, i + 1)?);
            let values = a.get(i + 2..i + 2 + n).ok_or_else(|| fail("DB_IncorrectArguments", "values missing"))?;
            self.set_prop(kind, obj, "", name, Some(values))?;
            i += 2 + n;
        }
        Ok(())
    }

    /// `[obj, name...]` -> `[obj, n, (name, count, values...)*]`
    fn get_props(&self, kind: Kind, a: &[String]) -> DevResult<Vec<String>> {
        let obj = at(a, 0)?;
        let mut out = vec![obj.to_string(), (a.len() - 1).to_string()];
        for name in &a[1..] {
            let values = self.get_prop(kind, obj, "", name)?;
            out.extend([name.clone(), values.len().to_string()]);
            out.extend(values);
        }
        Ok(out)
    }

    /// Attribute-like put, `[obj, n_attr, (attr, n_props, (prop, [count,] values...)*)*]`.
    fn put_sub_props(&self, kind: Kind, a: &[String], counted: bool) -> DevResult<()> {
        let (obj, mut i) = (at(a, 0)?, 2);
        for _ in 0..num(a, 1)? {
            let (sub, n_props) = (at(a, i)?.to_string(), num(a, i + 1)?);
            i += 2;
            for _ in 0..n_props {
                let name = at(a, i)?;
                let n = if counted { num(a, i + 1)? } else { 1 };
                let first = i + 1 + counted as usize;
                let values = a.get(first..first + n).ok_or_else(|| fail("DB_IncorrectArguments", "values missing"))?;
                self.set_prop(kind, obj, &sub, name, Some(values))?;
                i = first + n;
            }
        }
        Ok(())
    }

    /// Attribute-like get, `[obj, attr...]` -> `[obj, n_attr, (attr, n_props, (prop, [count,] values...)*)*]`.
    fn get_sub_props(&self, kind: Kind, a: &[String], counted: bool) -> DevResult<Vec<String>> {
        let obj = at(a, 0)?;
        let mut out = vec![obj.to_string(), (a.len() - 1).to_string()];
        for sub in &a[1..] {
            let names = self.names(kind, obj, sub, "*")?;
            out.extend([sub.clone(), names.len().to_string()]);
            for name in names {
                let values = self.get_prop(kind, obj, sub, &name)?;
                if counted {
                    out.extend([name, values.len().to_string()]);
                    out.extend(values);
                } else {
                    out.extend([name, values.join("\n")]);
                }
            }
        }
        Ok(out)
    }

    fn subs(&self, kind: Kind, obj: &str, wildcard: &str) -> DevResult<Vec<String>> {
        self.column(
            "SELECT DISTINCT sub FROM property WHERE kind = ?1 AND object = ?2 AND sub LIKE ?3 ORDER BY sub",
            params![kind.key(), obj, like(wildcard)],
        )
    }

    fn delete_sub(&self, kind: Kind, obj: &str, sub: &str) -> DevResult<()> {
        for name in self.names(kind, obj, sub, "*")? {
            self.set_prop(kind, obj, sub, &name, None)?;
        }
        Ok(())
    }

    /// Commands shared by the four attribute-like kinds.
    fn sub_command(&self, kind: Kind, op: &str, a: &[String]) -> DevResult<Value> {
        let obj = at(a, 0)?;
        Ok(match op {
            "Put" | "Put2" => void(self.put_sub_props(kind, a, op == "Put2"))?,
            "Get" => sa(self.get_sub_props(kind, a, false)?),
            "Get2" => sa(self.get_sub_props(kind, a, true)?),
            "List" => sa(self.subs(kind, obj, a.get(1).map_or("*", String::as_str))?),
            "Hist" => sa(self.history(kind, obj, at(a, 1)?, a.get(2).map_or("*", String::as_str), true)?),
            "DeleteProperty" => {
                for name in a.get(2..).unwrap_or_default() {
                    self.set_prop(kind, obj, at(a, 1)?, name, None)?;
                }
                Value::Void
            }
            _ => {
                // DeleteAll / delete the attribute or pipe: every property of the named objects
                for sub in &a[1..] {
                    self.delete_sub(kind, obj, sub)?;
                }
                Value::Void
            }
        })
    }

    // ---------------------------------------------------------------- dispatch

    fn run(&mut self, cmd: &str, arg: &Value) -> DevResult<Value> {
        let empty = vec![];
        let a: &[String] = match arg {
            Value::StringArray(v) => v,
            _ => &empty,
        };
        let s: &str = match arg {
            Value::String(s) => s,
            _ => "",
        };
        let list = |q: &str, p: &[&str]| -> DevResult<Value> { Ok(sa(self.column(q, params_from_iter(p.iter()))?)) };
        let attr_like = |name: &str| -> Option<(Kind, &'static str)> {
            let kinds = [
                ("DeviceAttribute", Kind::DeviceAttr),
                ("ClassAttribute", Kind::ClassAttr),
                ("DevicePipe", Kind::DevicePipe),
                ("ClassPipe", Kind::ClassPipe),
            ];
            let ops = [
                ("DbPut{}Property2", "Put2"),
                ("DbGet{}Property2", "Get2"),
                ("DbPut{}Property", "Put"),
                ("DbGet{}Property", "Get"),
                ("DbGet{}List", "List"),
                ("DbGet{}PropertyHist", "Hist"),
                ("DbDelete{}Property", "DeleteProperty"),
                ("DbDeleteAll{}Property", "DeleteAll"),
                ("DbDelete{}", "DeleteObject"),
            ];
            for (k, kind) in kinds {
                for (pattern, op) in ops {
                    if pattern.replace("{}", k) == name {
                        // pipes have a single property layout, with value counts
                        let pipe = matches!(kind, Kind::DevicePipe | Kind::ClassPipe);
                        let op = match op {
                            "Put" if pipe => "Put2",
                            "Get" if pipe => "Get2",
                            op => op,
                        };
                        return Some((kind, op));
                    }
                }
            }
            None
        };
        if let Some((kind, op)) = attr_like(cmd) {
            return self.sub_command(kind, op, a);
        }
        Ok(match cmd {
            // devices and servers
            "DbAddServer" => void(self.add_server(a))?,
            "DbAddDevice" => {
                let server = at(a, 0)?;
                self.add_device(server, &format!("dserver/{server}"), "DServer")?;
                void(self.add_device(server, at(a, 1)?, at(a, 2)?))?
            }
            "DbDeleteDevice" => void(self.delete_device(s))?,
            "DbDeleteServer" => {
                for dev in self.column("SELECT name FROM device WHERE server = ?1", [s])? {
                    self.delete_device(&dev)?;
                }
                self.exec("DELETE FROM server WHERE name = ?1", [s])?;
                Value::Void
            }
            "DbRenameServer" => {
                let (old, new) = (at(a, 0)?, at(a, 1)?);
                check_server(new)?;
                self.exec("UPDATE device SET server = ?2 WHERE server = ?1", [old, new])?;
                let (od, nd) = (format!("dserver/{old}"), format!("dserver/{new}"));
                let p: Vec<&str> = nd.split('/').collect();
                self.exec(
                    "UPDATE device SET name = ?2, family = ?3, member = ?4 WHERE name = ?1",
                    params![od, nd, p[1], p[2]],
                )?;
                self.exec("UPDATE server SET name = ?2 WHERE name = ?1", [old, new])?;
                Value::Void
            }
            "DbGetServerList" => {
                list("SELECT DISTINCT server FROM device WHERE server LIKE ?1 ORDER BY server", &[&like(s)])?
            }
            "DbGetServerNameList" => list(
                "SELECT DISTINCT substr(server, 1, instr(server, '/') - 1) AS e FROM device WHERE e LIKE ?1 ORDER BY e",
                &[&like(s)],
            )?,
            "DbGetInstanceNameList" => list(
                "SELECT DISTINCT substr(server, instr(server, '/') + 1) AS i FROM device WHERE server LIKE ?1 ORDER BY i",
                &[&format!("{s}/%")],
            )?,
            "DbGetDeviceList" => list(
                "SELECT name FROM device WHERE server LIKE ?1 AND class LIKE ?2 ORDER BY name",
                &[&like(at(a, 0)?), &like(at(a, 1)?)],
            )?,
            "DbGetDeviceClassList" => {
                let mut stmt =
                    self.conn.prepare("SELECT name, class FROM device WHERE server = ?1 ORDER BY name").map_err(sql)?;
                let rows =
                    stmt.query_map([s], |r| Ok([r.get::<_, String>(0)?, r.get::<_, String>(1)?])).map_err(sql)?;
                sa(rows.collect::<Result<Vec<_>, _>>().map_err(sql)?.concat())
            }
            "DbGetDeviceServerClassList" => {
                list("SELECT DISTINCT class FROM device WHERE server = ?1 ORDER BY class", &[s])?
            }
            "DbGetClassList" => {
                list("SELECT DISTINCT class FROM device WHERE class LIKE ?1 ORDER BY class", &[&like(s)])?
            }
            "DbGetClassForDevice" => {
                Value::String(self.one("SELECT class FROM device WHERE name = ?1", [s], Self::not_defined(s))?)
            }
            "DbGetClassInheritanceForDevice" => {
                sa(vec![self.one("SELECT class FROM device WHERE name = ?1", [s], Self::not_defined(s))?])
            }
            "DbGetDeviceDomainList" => {
                list("SELECT DISTINCT domain FROM device WHERE name LIKE ?1 ORDER BY domain", &[&like(s)])?
            }
            "DbGetDeviceFamilyList" => {
                list("SELECT DISTINCT family FROM device WHERE name LIKE ?1 ORDER BY family", &[&like(s)])?
            }
            "DbGetDeviceMemberList" => {
                list("SELECT DISTINCT member FROM device WHERE name LIKE ?1 ORDER BY member", &[&like(s)])?
            }
            "DbGetDeviceWideList" => list("SELECT name FROM device WHERE name LIKE ?1 ORDER BY name", &[&like(s)])?,
            "DbGetDeviceInfo" => self.device_info(s, true)?,

            // device, class and free properties
            "DbPutDeviceProperty" => void(self.put_props(Kind::Device, a))?,
            "DbGetDeviceProperty" => sa(self.get_props(Kind::Device, a)?),
            "DbDeleteDeviceProperty" | "DbDeleteClassProperty" | "DbDeleteProperty" => {
                let kind = match cmd {
                    "DbDeleteDeviceProperty" => Kind::Device,
                    "DbDeleteClassProperty" => Kind::Class,
                    _ => Kind::Free,
                };
                for name in a.get(1..).unwrap_or_default() {
                    self.set_prop(kind, at(a, 0)?, "", name, None)?;
                }
                Value::Void
            }
            "DbGetDevicePropertyList" => {
                sa(self.names(Kind::Device, at(a, 0)?, "", a.get(1).map_or("*", String::as_str))?)
            }
            "DbGetDevicePropertyHist" => sa(self.history(Kind::Device, at(a, 0)?, "", at(a, 1)?, false)?),
            "DbPutClassProperty" => void(self.put_props(Kind::Class, a))?,
            "DbGetClassProperty" => sa(self.get_props(Kind::Class, a)?),
            "DbGetClassPropertyList" => sa(self.names(Kind::Class, s, "", "*")?),
            "DbGetClassPropertyHist" => sa(self.history(Kind::Class, at(a, 0)?, "", at(a, 1)?, false)?),
            "DbPutProperty" => void(self.put_props(Kind::Free, a))?,
            "DbGetProperty" => sa(self.get_props(Kind::Free, a)?),
            "DbGetPropertyList" => sa(self.names(Kind::Free, at(a, 0)?, "", a.get(1).map_or("*", String::as_str))?),
            "DbGetPropertyHist" => sa(self.history(Kind::Free, at(a, 0)?, "", at(a, 1)?, false)?),
            "DbGetObjectList" => list(
                "SELECT DISTINCT object FROM property WHERE kind = 'free' AND object LIKE ?1 ORDER BY object",
                &[&like(s)],
            )?,

            // aliases
            "DbPutDeviceAlias" => {
                let (dev, alias) = (at(a, 0)?, at(a, 1)?);
                let owner = self
                    .conn
                    .query_row("SELECT name FROM device WHERE alias = ?1", [alias], |r| r.get::<_, String>(0))
                    .optional()
                    .map_err(sql)?;
                if owner.is_some_and(|o| !o.eq_ignore_ascii_case(dev)) {
                    return Err(fail("DB_AliasAlreadyDefined", format!("alias {alias} is already used")));
                }
                if self.exec("UPDATE device SET alias = ?2 WHERE name = ?1", [dev, alias])? == 0 {
                    return Err(Self::not_defined(dev));
                }
                Value::Void
            }
            "DbGetDeviceAlias" => Value::String(self.one(
                "SELECT alias FROM device WHERE name = ?1",
                [s],
                fail("DB_AliasNotDefined", format!("no alias for {s}")),
            )?),
            "DbGetAliasDevice" => Value::String(self.one(
                "SELECT name FROM device WHERE alias = ?1",
                [s],
                fail("DB_AliasNotDefined", format!("no device for alias {s}")),
            )?),
            "DbDeleteDeviceAlias" => void(self.exec("UPDATE device SET alias = NULL WHERE alias = ?1", [s]).map(drop))?,
            "DbGetDeviceAliasList" => list("SELECT alias FROM device WHERE alias LIKE ?1 ORDER BY alias", &[&like(s)])?,
            "DbPutAttributeAlias" => {
                let (name, alias) = (at(a, 0)?, at(a, 1)?);
                let owner = self
                    .conn
                    .query_row("SELECT name FROM attribute_alias WHERE alias = ?1", [alias], |r| r.get::<_, String>(0))
                    .optional()
                    .map_err(sql)?;
                if owner.is_some_and(|o| !o.eq_ignore_ascii_case(name)) {
                    return Err(fail("DB_AliasAlreadyDefined", format!("alias {alias} is already used")));
                }
                self.exec("INSERT OR REPLACE INTO attribute_alias VALUES(?1, ?2)", [alias, name])?;
                Value::Void
            }
            "DbGetAttributeAlias" | "DbGetAliasAttribute" => Value::String(self.one(
                "SELECT name FROM attribute_alias WHERE alias = ?1",
                [s],
                fail("DB_AliasNotDefined", format!("no attribute for alias {s}")),
            )?),
            "DbGetAttributeAlias2" => Value::String(self.one(
                "SELECT alias FROM attribute_alias WHERE name = ?1",
                [s],
                fail("DB_AliasNotDefined", format!("no alias for {s}")),
            )?),
            "DbDeleteAttributeAlias" => void(self.exec("DELETE FROM attribute_alias WHERE alias = ?1", [s]).map(drop))?,
            "DbGetAttributeAliasList" => {
                list("SELECT alias FROM attribute_alias WHERE alias LIKE ?1 ORDER BY alias", &[&like(s)])?
            }

            // hosts and servers
            "DbGetHostList" => list(
                "SELECT host FROM server WHERE host LIKE ?1 AND host != '' UNION SELECT host FROM device WHERE host LIKE ?1 AND host != '' ORDER BY 1",
                &[&like(s)],
            )?,
            "DbGetHostServerList" => list(
                "SELECT name FROM server WHERE host LIKE ?1
                 UNION SELECT server FROM device WHERE class = 'DServer' AND host LIKE ?1 AND host != '' ORDER BY 1",
                &[&like(s)],
            )?,
            "DbGetHostServersInfo" => {
                let mut stmt = self
                    .conn
                    .prepare("SELECT name, mode, level FROM server WHERE host = ?1 ORDER BY name")
                    .map_err(sql)?;
                let rows = stmt
                    .query_map([s], |r| {
                        Ok([r.get::<_, String>(0)?, r.get::<_, i64>(1)?.to_string(), r.get::<_, i64>(2)?.to_string()])
                    })
                    .map_err(sql)?;
                sa(rows.collect::<Result<Vec<_>, _>>().map_err(sql)?.concat())
            }
            "DbPutServerInfo" => {
                let (mode, level): (i64, i64) = (at(a, 2)?.parse().unwrap_or(0), at(a, 3)?.parse().unwrap_or(0));
                self.exec(
                    "INSERT OR REPLACE INTO server VALUES(?1, ?2, ?3, ?4)",
                    params![at(a, 0)?, at(a, 1)?, mode, level],
                )?;
                Value::Void
            }
            "DbGetServerInfo" => {
                let row = self
                    .conn
                    .query_row("SELECT host, mode, level FROM server WHERE name = ?1", [s], |r| {
                        Ok([r.get::<_, String>(0)?, r.get::<_, i64>(1)?.to_string(), r.get::<_, i64>(2)?.to_string()])
                    })
                    .optional()
                    .map_err(sql)?;
                let [host, mode, level] = row.unwrap_or_else(|| [String::new(), "0".into(), "0".into()]);
                sa(vec![s.to_string(), host, mode, level])
            }
            "DbDeleteServerInfo" => void(self.exec("DELETE FROM server WHERE name = ?1", [s]).map(drop))?,

            // export
            "DbExportDevice" => void(self.export(a))?,
            "DbUnExportDevice" => void(
                self.exec("UPDATE device SET exported = 0, stopped = datetime('now') WHERE name = ?1", [s]).map(drop),
            )?,
            "DbUnExportServer" => void(
                self.exec("UPDATE device SET exported = 0, stopped = datetime('now') WHERE server = ?1", [s]).map(drop),
            )?,
            "DbImportDevice" => self.device_info(s, false)?,
            "DbGetDeviceExportedList" => {
                list("SELECT name FROM device WHERE name LIKE ?1 AND exported = 1 ORDER BY name", &[&like(s)])?
            }
            "DbGetExportedDeviceListForClass" => {
                list("SELECT name FROM device WHERE class LIKE ?1 AND exported = 1 ORDER BY name", &[&like(s)])?
            }
            "DbExportEvent" => {
                let pid: i64 = at(a, 3)?.parse().unwrap_or(0);
                self.exec(
                    "INSERT OR REPLACE INTO event VALUES(?1, 1, ?2, ?3, ?4, ?5)",
                    params![at(a, 0)?, at(a, 1)?, at(a, 2)?, pid, at(a, 4)?],
                )?;
                Value::Void
            }
            "DbImportEvent" => {
                let row = self
                    .conn
                    .query_row("SELECT exported, pid, name, ior, version, host FROM event WHERE name = ?1", [s], |r| {
                        Ok((
                            r.get::<_, i32>(0)?,
                            r.get::<_, i32>(1)?,
                            (2..6).map(|i| r.get::<_, String>(i)).collect::<Result<Vec<_>, _>>()?,
                        ))
                    })
                    .optional()
                    .map_err(sql)?;
                let (e, pid, v) = row.ok_or_else(|| Self::not_defined(s))?;
                Value::LongStringArray(vec![e, pid], v)
            }
            "DbUnExportEvent" => void(self.exec("UPDATE event SET exported = 0 WHERE name = ?1", [s]).map(drop))?,

            // miscellaneous
            "DbInfo" => {
                let count = |q: &str| self.conn.query_row(q, [], |r| r.get::<_, i64>(0)).unwrap_or(0);
                sa(vec![
                    format!("TRust Database (SQLite {})", rusqlite::version()),
                    format!("Endpoint: {}", self.endpoint),
                    format!("Devices defined: {}", count("SELECT COUNT(*) FROM device")),
                    format!("Devices exported: {}", count("SELECT COUNT(*) FROM device WHERE exported = 1")),
                    format!("Servers defined: {}", count("SELECT COUNT(DISTINCT server) FROM device")),
                    format!("Property values: {}", count("SELECT COUNT(*) FROM property")),
                    format!(
                        "Aliases: {}",
                        count("SELECT COUNT(alias) FROM device") + count("SELECT COUNT(*) FROM attribute_alias")
                    ),
                ])
            }
            "DbMySqlSelect" => self.select(s)?,
            "DbGetCSDbServerList" => sa(vec![self.endpoint.clone()]),
            "DbGetForwardedAttributeListForDevice" => sa(vec![]),
            "ResetTimingValues" => Value::Void,
            "DbGetDataForServerCache" => self.run("DbGetDeviceClassList", &Value::String(at(a, 0)?.into()))?,
            _ => return Err(fail("API_CommandNotFound", format!("{cmd} is not a Database command"))),
        })
    }

    /// Read-only `SELECT` on the schema: `[rows, columns]`, values row by row.
    fn select(&self, query: &str) -> DevResult<Value> {
        let q = query.trim().trim_end_matches(';');
        if !q.to_ascii_lowercase().starts_with("select") || q.contains(';') {
            return Err(fail("DB_SQLError", "only a single SELECT is allowed"));
        }
        let mut stmt = self.conn.prepare(q).map_err(sql)?;
        let cols = stmt.column_count();
        let mut rows = stmt.query([]).map_err(sql)?;
        let (mut n, mut values) = (0, vec![]);
        while let Some(row) = rows.next().map_err(sql)? {
            n += 1;
            for i in 0..cols {
                values.push(match row.get::<_, Sql>(i).map_err(sql)? {
                    Sql::Null => String::new(),
                    Sql::Integer(v) => v.to_string(),
                    Sql::Real(v) => v.to_string(),
                    Sql::Text(v) => v,
                    Sql::Blob(v) => format!("{v:?}"),
                });
            }
        }
        Ok(Value::LongStringArray(vec![n, cols as i32], values))
    }
}

/// `Value::Void` once `r` succeeded.
fn void(r: DevResult<()>) -> DevResult<Value> {
    r.map(|_| Value::Void)
}

impl Device for DataBase {
    fn class_name(&self) -> String {
        "DataBase".into()
    }
    fn description(&self) -> String {
        "The Tango Database device (RFC-6)".into()
    }
    fn state(&mut self) -> DevState {
        match self.conn.query_row("SELECT 1", [], |r| r.get::<_, i64>(0)) {
            Ok(_) => DevState::On,
            Err(_) => DevState::Fault,
        }
    }
    fn commands(&self) -> Vec<CmdInfo> {
        COMMANDS
            .iter()
            .map(|(n, i, o)| CmdInfo { name: n.to_string(), in_type: *i, out_type: *o, ..Default::default() })
            .collect()
    }
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value> {
        self.run(cmd, &arg)
    }
}
