//! XML utilities for S3 responses

use quick_xml::se::Serializer;
use serde::Serialize;

/// XML serialization error
pub type XmlError = quick_xml::DeError;

/// Serialize a struct to XML with the XML declaration
pub fn to_xml<T: Serialize>(value: &T) -> Result<String, XmlError> {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let mut ser = Serializer::new(&mut xml);
    ser.indent(' ', 2);
    value.serialize(ser)?;
    Ok(xml)
}

/// Serialize a struct to XML without the declaration
pub fn to_xml_no_declaration<T: Serialize>(value: &T) -> Result<String, XmlError> {
    let mut xml = String::new();
    let mut ser = Serializer::new(&mut xml);
    ser.indent(' ', 2);
    value.serialize(ser)?;
    Ok(xml)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    #[serde(rename = "TestElement")]
    struct TestStruct {
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Value")]
        value: i32,
    }

    #[test]
    fn test_to_xml() {
        let test = TestStruct {
            name: "test".to_string(),
            value: 42,
        };

        let xml = to_xml(&test).unwrap();

        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<TestElement>"));
        assert!(xml.contains("<Name>test</Name>"));
        assert!(xml.contains("<Value>42</Value>"));
    }

    #[test]
    fn test_to_xml_no_declaration() {
        let test = TestStruct {
            name: "test".to_string(),
            value: 42,
        };

        let xml = to_xml_no_declaration(&test).unwrap();

        assert!(!xml.starts_with("<?xml"));
        assert!(xml.contains("<TestElement>"));
    }
}
