//! Generic forms (BROWSER.md §2): the widget and the text of every data type, and the parsing of
//! user input into a `Value`, inferred from metadata alone. Pure functions, unit tested.

use eframe::egui::Color32;
use trust::{AttrQuality, DataFormat, DataType, DevState, Value};

/// The input widget of a data type.
#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    /// No input: the command button alone.
    None,
    Check,
    /// Integer types: clamped to the type range and `[min, max]`.
    Integer {
        min: f64,
        max: f64,
    },
    /// Float types: parsed text with a range check.
    Float,
    Text,
    /// The 14 states.
    State,
    /// A DevEnum: the labels.
    Enum(Vec<String>),
    /// Arrays and SPECTRUM: comma-separated elements.
    List,
    /// Long/DoubleStringArray: `numbers ; strings`.
    Pair,
    /// Encoded, IMAGE and pipe blobs are not written from the GUI.
    ReadOnly,
}

/// The range of an integer type.
fn int_range(t: DataType) -> Option<(f64, f64)> {
    Some(match t {
        DataType::UChar => (0.0, u8::MAX as f64),
        DataType::Short => (i16::MIN as f64, i16::MAX as f64),
        DataType::UShort => (0.0, u16::MAX as f64),
        DataType::Long => (i32::MIN as f64, i32::MAX as f64),
        DataType::ULong => (0.0, u32::MAX as f64),
        DataType::Long64 => (i64::MIN as f64, i64::MAX as f64),
        DataType::ULong64 => (0.0, u64::MAX as f64),
        _ => return None,
    })
}

/// The input widget for a value of `t` in `format`, given the attribute's limits and enum labels.
pub fn input(t: DataType, format: DataFormat, limits: (Option<f64>, Option<f64>), labels: &[String]) -> Input {
    match format {
        DataFormat::Image => return Input::ReadOnly,
        DataFormat::Spectrum => return if t == DataType::Encoded { Input::ReadOnly } else { Input::List },
        DataFormat::Scalar => {}
    }
    if t == DataType::Short && !labels.is_empty() {
        return Input::Enum(labels.to_vec());
    }
    if let Some((lo, hi)) = int_range(t) {
        return Input::Integer { min: limits.0.map_or(lo, |m| m.max(lo)), max: limits.1.map_or(hi, |m| m.min(hi)) };
    }
    match t {
        DataType::Void => Input::None,
        DataType::Boolean => Input::Check,
        DataType::Float | DataType::Double => Input::Float,
        DataType::String => Input::Text,
        DataType::State => Input::State,
        DataType::LongStringArray | DataType::DoubleStringArray => Input::Pair,
        DataType::Encoded | DataType::PipeBlob | DataType::EncodedArray => Input::ReadOnly,
        _ => Input::List, // the array types, as command arguments
    }
}

/// `format` is a printf-like attribute format: `%6.2f`, `%d`, `%e`; only the precision of `f`
/// and `e` is honoured.
fn number(x: f64, format: &str) -> String {
    let precision = format.split('.').nth(1).and_then(|p| p.trim_end_matches(|c: char| c.is_alphabetic()).parse().ok());
    match (precision, format.ends_with('e')) {
        (Some(p), true) => format!("{x:.p$e}"),
        (Some(p), false) => format!("{x:.p$}"),
        (None, true) => format!("{x:e}"),
        (None, false) => format!("{x}"),
    }
}

/// The text of a value: numbers with `format` and `unit`, enum labels, states, lists. Plain
/// text only, which every font has.
pub fn display(v: &Value, format: &str, unit: &str, labels: &[String]) -> String {
    let unit = |s: String| if unit.is_empty() { s } else { format!("{s} {unit}") };
    let list = |items: Vec<String>| format!("[{}]", items.join(", "));
    match v {
        Value::Void => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::Short(i) if !labels.is_empty() => {
            labels.get(*i as usize).cloned().unwrap_or_else(|| format!("{i} (no label)"))
        }
        Value::String(s) => s.clone(),
        Value::State(s) => s.to_string(),
        Value::Encoded(e) => {
            let hex: Vec<String> = e.data.iter().take(8).map(|b| format!("{b:02x}")).collect();
            format!("{}, {} bytes: {}", e.format, e.data.len(), hex.join(" "))
        }
        Value::BooleanArray(v) => list(v.iter().map(|b| b.to_string()).collect()),
        Value::StringArray(v) => list(v.clone()),
        Value::StateArray(v) => list(v.iter().map(|s| s.to_string()).collect()),
        Value::EncodedArray(v) => format!("{} encoded values", v.len()),
        Value::LongStringArray(l, s) => {
            format!("{} ; {}", list(l.iter().map(|x| x.to_string()).collect()), list(s.clone()))
        }
        Value::DoubleStringArray(d, s) => {
            format!("{} ; {}", list(d.iter().map(|x| number(*x, format)).collect()), list(s.clone()))
        }
        Value::PipeBlob(b) => format!("{} {{{} elements}}", b.name, b.elements.len()),
        v if v.data_type().array() == v.data_type() && v.data_type() != DataType::Void => {
            unit(list(v.numbers().into_iter().map(|x| number(x, format)).collect()))
        }
        v => unit(v.numbers().first().map(|x| number(*x, format)).unwrap_or_default()),
    }
}

/// The text to edit a value with: what `parse` reads back.
pub fn edit_text(v: &Value) -> String {
    match v {
        Value::Boolean(b) => b.to_string(),
        Value::String(s) => s.clone(),
        Value::State(s) => s.to_string(),
        Value::StringArray(v) => v.join(", "),
        Value::BooleanArray(v) => v.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", "),
        Value::LongStringArray(l, s) => {
            format!("{} ; {}", l.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "), s.join(", "))
        }
        Value::DoubleStringArray(d, s) => {
            format!("{} ; {}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "), s.join(", "))
        }
        v => v.numbers().iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "),
    }
}

fn items(text: &str) -> Vec<&str> {
    text.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
}

/// Parses the text of a widget into a value of `t` (an array type for SPECTRUM), checking the
/// numbers against `[min, max]`.
pub fn parse(t: DataType, text: &str, min: Option<f64>, max: Option<f64>) -> Result<Value, String> {
    let text = text.trim();
    let bad = |what: &str| format!("{what:?} is not a {t:?}");
    macro_rules! num {
        ($ty:ty, $s:expr) => {{
            let s: &str = $s;
            let x: $ty = s.parse().map_err(|_| bad(s))?;
            let f = x as f64;
            if min.is_some_and(|m| f < m) || max.is_some_and(|m| f > m) {
                let range = format!(
                    "[{}, {}]",
                    min.map_or("-inf".into(), |m| m.to_string()),
                    max.map_or("inf".into(), |m| m.to_string())
                );
                return Err(format!("{s} is outside {range}"));
            }
            x
        }};
    }
    macro_rules! array {
        ($ty:ty) => {
            items(text).into_iter().map(|s| Ok(num!($ty, s))).collect::<Result<Vec<_>, String>>()?
        };
    }
    let boolean = |s: &str| match s.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(bad(s)),
    };
    let state = |s: &str| {
        let all = [
            DevState::On,
            DevState::Off,
            DevState::Close,
            DevState::Open,
            DevState::Insert,
            DevState::Extract,
            DevState::Moving,
            DevState::Standby,
            DevState::Fault,
            DevState::Init,
            DevState::Running,
            DevState::Alarm,
            DevState::Disable,
            DevState::Unknown,
        ];
        all.into_iter().find(|x| x.to_string().eq_ignore_ascii_case(s)).ok_or_else(|| bad(s))
    };
    let pair = || text.split_once(';').map(|(a, b)| (a, items(b).into_iter().map(String::from).collect()));
    Ok(match t {
        DataType::Void => Value::Void,
        DataType::Boolean => Value::Boolean(boolean(text)?),
        DataType::UChar => Value::UChar(num!(u8, text)),
        DataType::Short => Value::Short(num!(i16, text)),
        DataType::UShort => Value::UShort(num!(u16, text)),
        DataType::Long => Value::Long(num!(i32, text)),
        DataType::ULong => Value::ULong(num!(u32, text)),
        DataType::Long64 => Value::Long64(num!(i64, text)),
        DataType::ULong64 => Value::ULong64(num!(u64, text)),
        DataType::Float => Value::Float(num!(f32, text)),
        DataType::Double => Value::Double(num!(f64, text)),
        DataType::String => Value::String(text.into()),
        DataType::State => Value::State(state(text)?),
        DataType::CharArray => Value::CharArray(array!(u8)),
        DataType::ShortArray => Value::ShortArray(array!(i16)),
        DataType::UShortArray => Value::UShortArray(array!(u16)),
        DataType::LongArray => Value::LongArray(array!(i32)),
        DataType::ULongArray => Value::ULongArray(array!(u32)),
        DataType::Long64Array => Value::Long64Array(array!(i64)),
        DataType::ULong64Array => Value::ULong64Array(array!(u64)),
        DataType::FloatArray => Value::FloatArray(array!(f32)),
        DataType::DoubleArray => Value::DoubleArray(array!(f64)),
        DataType::BooleanArray => Value::BooleanArray(items(text).into_iter().map(boolean).collect::<Result<_, _>>()?),
        DataType::StringArray => Value::StringArray(items(text).into_iter().map(String::from).collect()),
        DataType::StateArray => Value::StateArray(items(text).into_iter().map(state).collect::<Result<_, _>>()?),
        DataType::LongStringArray => {
            let (l, s) = pair().ok_or_else(|| "expected numbers ; strings".to_string())?;
            Value::LongStringArray(items(l).into_iter().map(|s| Ok(num!(i32, s))).collect::<Result<_, String>>()?, s)
        }
        DataType::DoubleStringArray => {
            let (d, s) = pair().ok_or_else(|| "expected numbers ; strings".to_string())?;
            Value::DoubleStringArray(items(d).into_iter().map(|s| Ok(num!(f64, s))).collect::<Result<_, String>>()?, s)
        }
        DataType::Encoded | DataType::EncodedArray | DataType::PipeBlob => {
            return Err(format!("{t:?} is not written from the browser"));
        }
    })
}

/// An IMAGE attribute as a greyscale heat map, `width` pixels wide: the smallest value is black,
/// the largest white.
pub fn heat_map(values: &[f64], width: usize) -> eframe::egui::ColorImage {
    let width = width.max(1);
    let height = values.len().div_ceil(width).max(1);
    let (lo, hi) = values.iter().fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), x| (lo.min(*x), hi.max(*x)));
    let span = if hi > lo { hi - lo } else { 1.0 };
    let mut pixels = vec![Color32::BLACK; width * height];
    for (p, x) in pixels.iter_mut().zip(values) {
        let g = ((x - lo) / span * 255.0).round() as u8;
        *p = Color32::from_gray(g);
    }
    eframe::egui::ColorImage::new([width, height], pixels)
}

/// Tango colours of the states.
pub fn state_color(s: DevState) -> Color32 {
    match s {
        DevState::On | DevState::Open | DevState::Extract => Color32::from_rgb(0, 200, 0),
        DevState::Off | DevState::Close | DevState::Insert => Color32::WHITE,
        DevState::Moving => Color32::from_rgb(128, 160, 255),
        DevState::Standby => Color32::YELLOW,
        DevState::Fault => Color32::RED,
        DevState::Init => Color32::from_rgb(204, 204, 122),
        DevState::Running => Color32::from_rgb(0, 125, 0),
        DevState::Alarm => Color32::from_rgb(255, 140, 0),
        DevState::Disable => Color32::from_rgb(255, 0, 255),
        DevState::Unknown => Color32::GRAY,
    }
}

pub fn quality_color(q: AttrQuality) -> Color32 {
    match q {
        AttrQuality::Valid => Color32::from_rgb(0, 200, 0),
        AttrQuality::Alarm => Color32::RED,
        AttrQuality::Warning => Color32::from_rgb(255, 140, 0),
        AttrQuality::Changing => Color32::from_rgb(128, 160, 255),
        AttrQuality::Invalid => Color32::GRAY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DataType as T;

    fn labels() -> Vec<String> {
        ["OFF", "ON"].map(String::from).to_vec()
    }

    #[test]
    fn widgets() {
        let none = (None, None);
        let scalar = |t| input(t, DataFormat::Scalar, none, &[]);
        assert_eq!(scalar(T::Void), Input::None);
        assert_eq!(scalar(T::Boolean), Input::Check);
        assert_eq!(scalar(T::Short), Input::Integer { min: -32768.0, max: 32767.0 });
        assert_eq!(
            input(T::UChar, DataFormat::Scalar, (Some(-5.0), Some(10.0)), &[]),
            Input::Integer { min: 0.0, max: 10.0 }
        );
        assert_eq!(scalar(T::Double), Input::Float);
        assert_eq!(scalar(T::String), Input::Text);
        assert_eq!(scalar(T::State), Input::State);
        assert_eq!(input(T::Short, DataFormat::Scalar, none, &labels()), Input::Enum(labels()));
        assert_eq!(scalar(T::Encoded), Input::ReadOnly);
        assert_eq!(input(T::Long, DataFormat::Spectrum, none, &[]), Input::List);
        assert_eq!(input(T::Long, DataFormat::Image, none, &[]), Input::ReadOnly);
        assert_eq!(scalar(T::DoubleArray), Input::List, "an array command argument");
        assert_eq!(scalar(T::LongStringArray), Input::Pair);
        assert_eq!(scalar(T::PipeBlob), Input::ReadOnly);
    }

    #[test]
    fn texts() {
        assert_eq!(display(&Value::Double(12.5), "%6.2f", "mm", &[]), "12.50 mm");
        assert_eq!(display(&Value::Double(1500.0), "%.1e", "", &[]), "1.5e3");
        assert_eq!(display(&Value::Long(3), "%d", "", &[]), "3");
        assert_eq!(display(&Value::Short(1), "", "", &labels()), "ON");
        assert_eq!(display(&Value::Short(7), "", "", &labels()), "7 (no label)");
        assert_eq!(display(&Value::Boolean(true), "", "", &[]), "true");
        assert_eq!(display(&Value::State(DevState::Moving), "", "", &[]), "MOVING");
        assert_eq!(display(&Value::LongArray(vec![1, 2, 3]), "", "", &[]), "[1, 2, 3]");
        assert_eq!(display(&Value::StringArray(vec!["a".into(), "b".into()]), "", "", &[]), "[a, b]");
        assert_eq!(display(&Value::LongStringArray(vec![1], vec!["x".into()]), "", "", &[]), "[1] ; [x]");
        let enc = Value::Encoded(trust::DevEncoded { format: "raw".into(), data: vec![1, 255] });
        assert_eq!(display(&enc, "", "", &[]), "raw, 2 bytes: 01 ff");
    }

    #[test]
    fn parsing() {
        assert_eq!(parse(T::Double, " 3.5 ", None, None), Ok(Value::Double(3.5)));
        assert_eq!(parse(T::Short, "-2", None, None), Ok(Value::Short(-2)));
        assert!(parse(T::Short, "70000", None, None).is_err(), "out of the type range");
        assert!(parse(T::UShort, "-1", None, None).is_err());
        assert!(parse(T::Double, "abc", None, None).is_err());
        assert_eq!(parse(T::Double, "11", None, Some(10.0)), Err("11 is outside [-inf, 10]".into()));
        assert_eq!(parse(T::Boolean, "TRUE", None, None), Ok(Value::Boolean(true)));
        assert_eq!(parse(T::State, "moving", None, None), Ok(Value::State(DevState::Moving)));
        assert_eq!(parse(T::String, "hello", None, None), Ok(Value::String("hello".into())));
        assert_eq!(parse(T::LongArray, "1, 2,3", None, None), Ok(Value::LongArray(vec![1, 2, 3])));
        assert!(parse(T::LongArray, "1, x", None, None).is_err(), "element-wise validation");
        assert!(parse(T::DoubleArray, "1, 20", None, Some(10.0)).is_err(), "element-wise range");
        assert_eq!(parse(T::StringArray, "a, b", None, None), Ok(Value::StringArray(vec!["a".into(), "b".into()])));
        assert_eq!(
            parse(T::LongStringArray, "1, 2 ; a", None, None),
            Ok(Value::LongStringArray(vec![1, 2], vec!["a".into()]))
        );
        assert!(parse(T::LongStringArray, "1, 2", None, None).is_err());
        assert!(parse(T::Encoded, "x", None, None).is_err());
        assert_eq!(parse(T::Void, "", None, None), Ok(Value::Void));
    }

    #[test]
    fn edit_text_round_trips() {
        let values = [
            Value::Double(2.5),
            Value::Long(-3),
            Value::Boolean(false),
            Value::String("s".into()),
            Value::State(DevState::On),
            Value::DoubleArray(vec![1.0, 2.5]),
            Value::StringArray(vec!["a".into(), "b".into()]),
            Value::DoubleStringArray(vec![1.5], vec!["x".into()]),
        ];
        for v in values {
            assert_eq!(parse(v.data_type(), &edit_text(&v), None, None), Ok(v.clone()), "{v:?}");
        }
    }

    #[test]
    fn heat_maps() {
        let image = heat_map(&[0.0, 5.0, 10.0, 10.0, 0.0, 5.0], 3);
        assert_eq!(image.size, [3, 2]);
        assert_eq!(image.pixels[0], Color32::from_gray(0), "smallest: black");
        assert_eq!(image.pixels[2], Color32::from_gray(255), "largest: white");
        assert_eq!(image.pixels[1], Color32::from_gray(128));
        assert_eq!(heat_map(&[7.0, 7.0], 2).pixels[0], Color32::from_gray(0), "a flat image");
        assert_eq!(heat_map(&[], 0).size, [1, 1]);
    }

    #[test]
    fn colours() {
        assert_eq!(state_color(DevState::Fault), Color32::RED);
        assert_eq!(quality_color(AttrQuality::Alarm), Color32::RED);
        assert_ne!(quality_color(AttrQuality::Valid), quality_color(AttrQuality::Invalid));
    }
}
