//! Where a server finds its devices and properties: nothing, a Database or a Database file.

use crate::db::Database;
use crate::dbfile::DbFile;
use crate::types::{AttrInfo, DevResult};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) type Raw = HashMap<String, Vec<String>>;

#[derive(Clone, Default)]
pub(crate) enum Store {
    #[default]
    None,
    /// The connection, and the TANGO_HOST to open a new one from another runtime.
    Db(Database, String),
    File(Arc<DbFile>),
}

impl Store {
    /// `(device, class)` registered for `server`.
    pub(crate) async fn devices(&self, server: &str) -> DevResult<Vec<(String, String)>> {
        Ok(match self {
            Self::None => vec![],
            Self::Db(db, _) => db.server_devices(server).await?,
            Self::File(f) => f
                .devices
                .iter()
                .filter(|d| d.0.eq_ignore_ascii_case(server))
                .map(|d| (d.2.clone(), d.1.clone()))
                .collect(),
        })
    }

    /// Raw values of the `names` properties of a device, device values over class values.
    pub(crate) async fn properties(&self, device: &str, class: &str, names: &[String]) -> DevResult<Raw> {
        let pick = |m: &std::collections::BTreeMap<(String, String), Vec<String>>, owner: &str| -> Raw {
            m.iter()
                .filter(|((o, p), _)| o.eq_ignore_ascii_case(owner) && names.iter().any(|n| n.eq_ignore_ascii_case(p)))
                .map(|((_, p), v)| (p.to_lowercase(), v.clone()))
                .collect()
        };
        let (mut class_props, device_props) = match self {
            Self::None => return Ok(Raw::new()),
            Self::Db(..) if names.is_empty() => return Ok(Raw::new()),
            Self::Db(db, _) => (db.properties(class, names, true).await?, db.properties(device, names, false).await?),
            Self::File(f) => (pick(&f.class_props, class), pick(&f.device_props, device)),
        };
        class_props.extend(device_props);
        Ok(class_props)
    }

    /// `properties` from synchronous code (the Init command): runs on a short-lived thread with
    /// its own runtime and Database connection, so it works whatever runtime called it.
    pub(crate) fn properties_blocking(&self, device: &str, class: &str, names: &[String]) -> DevResult<Raw> {
        let store = self.clone();
        let read = || {
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| -> DevResult<Raw> {
                    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                    rt.block_on(async {
                        let store = match store {
                            Self::Db(_, host) => Self::Db(Database::connect(&host).await?, host),
                            other => other,
                        };
                        store.properties(device, class, names).await
                    })
                });
                worker.join().unwrap_or_else(|_| {
                    Err(crate::types::DevFailed::new("API_InternalError", "property reader panicked", "Init"))
                })
            })
        };
        // on a multi-thread worker, hand its queued tasks to another worker while waiting: one of
        // them may be the reply we wait for (an in-process Database)
        match tokio::runtime::Handle::try_current() {
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(read)
            }
            _ => read(),
        }
    }

    /// `attribute -> property -> values` of a device.
    pub(crate) async fn attribute_properties(&self, device: &str, attrs: &[String]) -> DevResult<HashMap<String, Raw>> {
        Ok(match self {
            Self::None => HashMap::new(),
            Self::Db(db, _) => db.attribute_properties(device, attrs).await?,
            Self::File(f) => {
                let mut out: HashMap<String, Raw> = HashMap::new();
                for ((d, a, p), v) in &f.attr_props {
                    if d.eq_ignore_ascii_case(device) {
                        out.entry(a.to_lowercase()).or_default().insert(p.to_lowercase(), v.clone());
                    }
                }
                out
            }
        })
    }

    /// Persists the editable properties of an attribute (Database only, in the background).
    pub(crate) fn save_attribute(&self, device: &str, a: &AttrInfo) {
        let Self::Db(db, _) = self else { return };
        let text = |s: &str| (!s.is_empty()).then(|| vec![s.to_string()]);
        let num = |v: Option<f64>| v.map(|x| vec![x.to_string()]);
        let props = [
            ("description", text(&a.description)),
            ("label", text(&a.label)),
            ("unit", text(&a.unit)),
            ("format", text(&a.format)),
            ("min_value", num(a.min_value)),
            ("max_value", num(a.max_value)),
            ("min_alarm", num(a.min_alarm)),
            ("max_alarm", num(a.max_alarm)),
            ("min_warning", num(a.min_warning)),
            ("max_warning", num(a.max_warning)),
        ]
        .map(|(n, v)| (n.to_string(), v));
        let (db, device, attr) = (db.clone(), device.to_string(), a.name.clone());
        tokio::spawn(async move { db.put_attribute_properties(&device, &attr, &props).await });
    }
}

/// Applies stored attribute properties over the static ones.
pub(crate) fn apply_attribute(a: &mut AttrInfo, props: &Raw) {
    let first = |n: &str| props.get(n).and_then(|v| v.first()).cloned();
    let num = |n: &str| first(n).and_then(|v| v.trim().parse::<f64>().ok());
    for (field, name) in
        [(&mut a.description, "description"), (&mut a.label, "label"), (&mut a.unit, "unit"), (&mut a.format, "format")]
    {
        if let Some(v) = first(name) {
            *field = v;
        }
    }
    for (field, name) in [
        (&mut a.min_value, "min_value"),
        (&mut a.max_value, "max_value"),
        (&mut a.min_alarm, "min_alarm"),
        (&mut a.max_alarm, "max_alarm"),
        (&mut a.min_warning, "min_warning"),
        (&mut a.max_warning, "max_warning"),
    ] {
        if let Some(v) = num(name) {
            *field = Some(v);
        }
    }
}
