pub mod stats {
    include!(concat!(env!("OUT_DIR"), "/stats.rs"));
}

pub mod logproto {
    include!(concat!(env!("OUT_DIR"), "/logproto.rs"));
}

pub mod util {
    use super::logproto;
    use prost::Message;
    use std::collections::HashMap;

    const NANOS_RANGE: i64 = 1_000_000_000;

    // (<Timestamp in nanos>, <Line>)
    pub struct Entry(pub i64, pub String);

    impl From<Entry> for logproto::EntryAdapter {
        fn from(entry: Entry) -> Self {
            logproto::EntryAdapter {
                timestamp: Some(prost_types::Timestamp {
                    seconds: entry.0 / NANOS_RANGE,
                    nanos: (entry.0 % NANOS_RANGE) as i32,
                }),
                line: entry.1,
            }
        }
    }

    // (<Labels>, <Lines>)
    pub struct Batch(pub HashMap<String, String>, pub Vec<Entry>);

    impl From<Batch> for logproto::StreamAdapter {
        fn from(batch: Batch) -> Self {
            let labels = encode_labels_map_to_string(&batch.0);
            let entries: Vec<logproto::EntryAdapter> =
                batch.1.into_iter().map(|entry| entry.into()).collect();

            logproto::StreamAdapter {
                labels,
                entries,
                hash: 0,
            }
        }
    }

    impl Batch {
        pub fn encode(self) -> Vec<u8> {
            let stream: logproto::StreamAdapter = self.into();
            let push_request = logproto::PushRequest {
                streams: vec![stream],
            };
            let buf = push_request.encode_to_vec();
            let mut encoder = snap::raw::Encoder::new();
            encoder.compress_vec(&buf).expect("out of memory")
        }
    }

    const RESERVED_LABEL_TENANT_ID: &str = "__tenant_id__";
    const RESERVED_LABELS: [&str; 1] = [RESERVED_LABEL_TENANT_ID];

    // ref: https://github.com/grafana/loki/blob/65c6e254bd22151ab7fc84ec46e13eee2e354aa0/clients/pkg/promtail/client/batch.go#L61-L75
    pub fn encode_labels_map_to_string(labels: &HashMap<String, String>) -> String {
        let mut labels: Vec<String> = labels
            .iter()
            .filter(|(k, _)| !RESERVED_LABELS.contains(&k.as_str()))
            .map(|(k, v)| format!("{}={:?}", k, v))
            .collect();
        labels.sort();
        return format!("{{{}}}", labels.join(", "));
    }

    pub fn decode_labels_map_to_string(labels: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let list = rem_first_and_last(labels).split(", ");

        for item in list {
            let mut kv = item.split("=");
            let key = kv.next();
            let value = kv.next();

            match value {
                None => (),
                Some(str) => {
                    map.insert(key.unwrap().into(), unescape_string_literal(str));
                }
            }
        }

        map
    }

    fn rem_first_and_last(value: &str) -> &str {
        let mut chars = value.chars();
        chars.next();
        chars.next_back();
        chars.as_str()
    }

    fn unescape_string_literal(mut s: &str) -> String {
        s = rem_first_and_last(s);
        let mut string = String::with_capacity(s.len());
        while let Some(i) = s.bytes().position(|b| b == b'\\') {
            let next = s.as_bytes()[i + 1];
            if next == b'\n' {
                // Remove the \n and any ensuing spaces or tabs
                string.push_str(&s[..i]);
                let remaining = &s[i + 2..];
                let whitespace: usize = remaining
                    .chars()
                    .take_while(|c| c.is_whitespace())
                    .map(|c| c.len_utf8())
                    .sum();
                s = &s[i + whitespace + 2..];
            } else {
                let c = match next {
                    b'\'' => '\'',
                    b'"' => '"',
                    b'\\' => '\\',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'{' => '{',
                    _ => unimplemented!("invalid escape"),
                };

                string.push_str(&s[..i]);
                string.push(c);
                s = &s[i + 2..];
            }
        }

        string.push_str(s);
        string
    }
}

#[cfg(test)]
mod tests {
    use super::util;
    use crate::util::{Batch, Entry};
    use chrono::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn encode_labels() {
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("__tenant_id__".into(), "tenant_id".into());
        labels.insert("agent".into(), "vector".into());
        labels.insert("host".into(), "localhost".into());
        labels.insert("file".into(), "/path/to/log".into());
        labels.insert("job".into(), "file_logs".into());
        let s = util::encode_labels_map_to_string(&labels);
        assert_eq!(
            s,
            r#"{agent="vector", file="/path/to/log", host="localhost", job="file_logs"}"#
        );
    }

    #[test]
    fn encode_batch() {
        let ts1 = Utc.timestamp(1640244790, 0);
        let entry1 = Entry(ts1.timestamp_nanos(), "hello".into());
        let ts2 = Utc.timestamp(1640244791, 0);
        let entry2 = Entry(ts2.timestamp_nanos(), "world".into());
        let labels = vec![("source".into(), "protobuf-test".into())]
            .into_iter()
            .collect();
        let batch = Batch(labels, vec![entry1, entry2]);
        // generated by test codes from promtail
        let expect = vec![
            62, 176, 10, 60, 10, 24, 123, 115, 111, 117, 114, 99, 101, 61, 34, 112, 114, 111, 116,
            111, 98, 117, 102, 45, 116, 101, 115, 116, 34, 125, 18, 15, 10, 6, 8, 182, 204, 144,
            142, 6, 18, 5, 104, 101, 108, 108, 111, 5, 17, 44, 183, 204, 144, 142, 6, 18, 5, 119,
            111, 114, 108, 100,
        ];
        let buf = batch.encode();
        assert_eq!(expect, buf);
    }
}
