//! `Demo`: a small device to try the browser on (`DemoServer` binary), also used by its tests.

use trust::{
    AttrInfo, CmdInfo, DataFormat, DataType as T, DevResult, DevState, Device, PipeBlob, PipeInfo, PropInfo,
    Properties, Server, Value, Writable,
};

pub struct Demo {
    position: f64,
    mode: i16,
    label: String,
    blob: PipeBlob,
}

impl Default for Demo {
    fn default() -> Self {
        let blob = PipeBlob { name: "settings".into(), elements: vec![("gain".into(), Value::Double(2.0))] };
        Self { position: 0.0, mode: 0, label: String::new(), blob }
    }
}

impl Device for Demo {
    fn class_name(&self) -> String {
        "Demo".into()
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn properties(&self) -> Vec<PropInfo> {
        vec![PropInfo {
            name: "Label".into(),
            data_type: T::String,
            description: "shown by the label attribute".into(),
            default: Some(vec!["demo".into()]),
            mandatory: false,
        }]
    }
    fn init(&mut self, props: &Properties) -> DevResult<()> {
        if let Some(Value::String(l)) = props.get("Label") {
            self.label = l.clone();
        }
        Ok(())
    }
    fn commands(&self) -> Vec<CmdInfo> {
        let cmd =
            |name: &str, in_type, out_type| CmdInfo { name: name.into(), in_type, out_type, ..Default::default() };
        vec![cmd("Twice", T::Double, T::Double), cmd("Reset", T::Void, T::Void), cmd("Crash", T::Void, T::Void)]
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        let attr =
            |name: &str, data_type, writable| AttrInfo { name: name.into(), data_type, writable, ..Default::default() };
        vec![
            AttrInfo {
                unit: "mm".into(),
                format: "%.2f".into(),
                max_value: Some(100.0),
                max_alarm: Some(50.0),
                ..attr("position", T::Double, Writable::ReadWrite)
            },
            AttrInfo {
                enum_labels: ["OFF", "ON", "AUTO"].map(String::from).to_vec(),
                ..attr("mode", T::Short, Writable::ReadWrite)
            },
            AttrInfo { data_format: DataFormat::Spectrum, max_dim_x: 16, ..attr("wave", T::Double, Writable::Read) },
            attr("label", T::String, Writable::Read),
            AttrInfo {
                data_format: DataFormat::Image,
                max_dim_x: 4,
                max_dim_y: 3,
                ..attr("frame", T::Short, Writable::Read)
            },
        ]
    }
    fn pipes(&self) -> Vec<PipeInfo> {
        vec![PipeInfo { name: "settings".into(), writable: true, ..Default::default() }]
    }
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value> {
        match (cmd, arg) {
            ("Twice", Value::Double(x)) => Ok(Value::Double(2.0 * x)),
            ("Crash", _) => std::process::exit(3),
            _ => {
                self.position = 0.0;
                Ok(Value::Void)
            }
        }
    }
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value> {
        Ok(match attr {
            "position" => Value::Double(self.position),
            "mode" => Value::Short(self.mode),
            "wave" => Value::DoubleArray((0..16).map(|i| (i as f64 / 2.5).sin()).collect()),
            "frame" => Value::ShortArray((0..12).collect()),
            _ => Value::String(self.label.clone()),
        })
    }
    fn write_attribute(&mut self, attr: &str, value: Value) -> DevResult<()> {
        match value {
            Value::Double(v) => self.position = v,
            Value::Short(v) if attr == "mode" => self.mode = v,
            _ => {}
        }
        Ok(())
    }
    fn read_pipe(&mut self, _pipe: &str) -> DevResult<PipeBlob> {
        Ok(self.blob.clone())
    }
    fn write_pipe(&mut self, _pipe: &str, blob: PipeBlob) -> DevResult<()> {
        self.blob = blob;
        Ok(())
    }
}

/// `DemoServer <instance>`: one `Demo` device per device registered in the Database.
pub fn server(args: Vec<String>) -> DevResult<Server> {
    Ok(Server::from_args("DemoServer", args)?.class("Demo", || Box::new(Demo::default())))
}
