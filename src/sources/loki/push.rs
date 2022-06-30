use std::{collections::HashMap, net::SocketAddr};

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use prost::Message;
use serde::{Deserialize, Serialize};
use warp::http::{HeaderMap, StatusCode};

// use super::parser;
use crate::{
    config::{
        self, log_schema, AcknowledgementsConfig, GenerateConfig, Output, SourceConfig, SourceContext,
        SourceDescription,
    },
    event::Event,
    internal_events::LokiParseError,
    serde::bool_or_struct,
    sources::{
        self,
        http::HttpMethod,
        util::{decode, ErrorMessage, HttpSource, HttpSourceAuthConfig},
    },
    tls::TlsEnableableConfig,
};

const SOURCE_NAME: &str = "loki_push";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LokiPushConfig {
    address: SocketAddr,

    tls: Option<TlsEnableableConfig>,

    auth: Option<HttpSourceAuthConfig>,

    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: AcknowledgementsConfig,
}

inventory::submit! {
    SourceDescription::new::<LokiPushConfig>(SOURCE_NAME)
}

impl GenerateConfig for LokiPushConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "127.0.0.1:3100".parse().unwrap(),
            tls: None,
            auth: None,
            acknowledgements: AcknowledgementsConfig::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "loki_push")]
impl SourceConfig for LokiPushConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let source = PushSource;
        source.run(
            self.address,
            "",
            HttpMethod::Post,
            true,
            &self.tls,
            &self.auth,
            cx,
            self.acknowledgements,
        )
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(config::DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        SOURCE_NAME
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PushSource;

impl PushSource {
    fn decode_body(&self, tenant_id: Option<&str>, body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
        let request = loki_logproto::logproto::PushRequest::decode(body).map_err(|error| {
            emit!(LokiParseError {
                error: error.clone()
            });
            ErrorMessage::new(
                StatusCode::BAD_REQUEST,
                format!("Could not decode write request: {}", error),
            )
        })?;

        Ok(self.parse_request(tenant_id, request))
    }

    fn parse_request(&self, tenant_id: Option<&str>, request: loki_logproto::logproto::PushRequest) -> Vec<Event> {
        let mut result = Vec::new();

        for stream in request.streams {
            let labels = loki_logproto::util::decode_labels_map_to_string(&stream.labels);

            for entry in stream.entries {
                let mut event = Event::new_empty_log();
                let log = event.as_mut_log();

                log.insert(log_schema().source_type_key(), Bytes::from(SOURCE_NAME));
                log.insert(log_schema().message_key(), entry.line);

                // tenant id
                if let Some(tenant) = tenant_id {
                    log.insert("__tenant_id__", tenant.to_owned());
                }

                // timestamp
                match entry.timestamp {
                    None => (),
                    Some(prost_types::Timestamp{seconds, nanos}) => {
                        let time = Utc.timestamp(
                            seconds,
                            nanos as u32
                        );

                        log.insert(log_schema().timestamp_key(), time);
                    }
                }

                // labels
                for (key, value) in labels.iter() {
                    log.insert(format!("labels.{}", key).as_str(), value.to_owned());
                }

                result.push(event);
            }
        }

        result
    }
}

impl HttpSource for PushSource {
    fn build_events(
        &self,
        mut body: Bytes,
        header_map: HeaderMap,
        _query_parameters: HashMap<String, String>,
        _full_path: &str,
    ) -> Result<Vec<Event>, ErrorMessage> {
        // If `Content-Encoding` header isn't `snappy` HttpSource won't decode it for us
        // se we need to.
        if header_map
            .get("Content-Encoding")
            .map(|header| header.as_ref())
            != Some(&b"snappy"[..])
        {
            body = decode(&Some("snappy".to_string()), body)?;
        }

        let tenant_id = header_map
            .get("X-Scope-OrgID")
            .map(|header| header.to_str().ok())
            .unwrap_or(None);

        let events = self.decode_body(tenant_id, body)?;
        Ok(events)
    }
}

// #[cfg(test)]
// mod test {
//     use chrono::{SubsecRound as _, Utc};
//     use vector_core::event::{EventStatus, Metric, MetricKind, MetricValue};

//     use super::*;
//     use crate::{
//         config::{SinkConfig, SinkContext},
//         sinks::prometheus::remote_write::RemoteWriteConfig,
//         test_util::{
//             self,
//             components::{assert_source_compliance, HTTP_PUSH_SOURCE_TAGS},
//         },
//         tls::MaybeTlsSettings,
//         SourceSender,
//     };

//     #[test]
//     fn generate_config() {
//         crate::test_util::test_generate_config::<PrometheusRemoteWriteConfig>();
//     }

//     #[tokio::test]
//     async fn receives_metrics_over_http() {
//         receives_metrics(None).await;
//     }

//     #[tokio::test]
//     async fn receives_metrics_over_https() {
//         receives_metrics(Some(TlsEnableableConfig::test_config())).await;
//     }

//     async fn receives_metrics(tls: Option<TlsEnableableConfig>) {
//         assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
//             let address = test_util::next_addr();
//             let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);

//             let proto = MaybeTlsSettings::from_config(&tls, true)
//                 .unwrap()
//                 .http_protocol_name();
//             let source = PrometheusRemoteWriteConfig {
//                 address,
//                 auth: None,
//                 tls: tls.clone(),
//                 acknowledgements: AcknowledgementsConfig::default(),
//             };
//             let source = source
//                 .build(SourceContext::new_test(tx, None))
//                 .await
//                 .unwrap();
//             tokio::spawn(source);

//             let sink = RemoteWriteConfig {
//                 endpoint: format!("{}://localhost:{}/", proto, address.port()),
//                 tls: tls.map(|tls| tls.options),
//                 ..Default::default()
//             };
//             let (sink, _) = sink
//                 .build(SinkContext::new_test())
//                 .await
//                 .expect("Error building config.");

//             let events = make_events();
//             let events_copy = events.clone();
//             let mut output = test_util::spawn_collect_ready(
//                 async move {
//                     sink.run_events(events_copy).await.unwrap();
//                 },
//                 rx,
//                 1,
//             )
//             .await;

//             // The MetricBuffer used by the sink may reorder the metrics, so
//             // put them back into order before comparing.
//             output.sort_unstable_by_key(|event| event.as_metric().name().to_owned());

//             vector_common::assert_event_data_eq!(events, output);
//         })
//         .await;
//     }

//     fn make_events() -> Vec<Event> {
//         let timestamp = || Utc::now().trunc_subsecs(3);
//         vec![
//             Metric::new(
//                 "counter_1",
//                 MetricKind::Absolute,
//                 MetricValue::Counter { value: 42.0 },
//             )
//             .with_timestamp(Some(timestamp()))
//             .into(),
//             Metric::new(
//                 "gauge_2",
//                 MetricKind::Absolute,
//                 MetricValue::Gauge { value: 41.0 },
//             )
//             .with_timestamp(Some(timestamp()))
//             .into(),
//             Metric::new(
//                 "histogram_3",
//                 MetricKind::Absolute,
//                 MetricValue::AggregatedHistogram {
//                     buckets: vector_core::buckets![ 2.3 => 11, 4.2 => 85 ],
//                     count: 96,
//                     sum: 156.2,
//                 },
//             )
//             .with_timestamp(Some(timestamp()))
//             .into(),
//             Metric::new(
//                 "summary_4",
//                 MetricKind::Absolute,
//                 MetricValue::AggregatedSummary {
//                     quantiles: vector_core::quantiles![ 0.1 => 1.2, 0.5 => 3.6, 0.9 => 5.2 ],
//                     count: 23,
//                     sum: 8.6,
//                 },
//             )
//             .with_timestamp(Some(timestamp()))
//             .into(),
//         ]
//     }
// }

// #[cfg(all(test, feature = "prometheus-integration-tests"))]
// mod integration_tests {
//     use tokio::time::Duration;

//     use super::*;
//     use crate::test_util::components::{run_and_assert_source_compliance, HTTP_PUSH_SOURCE_TAGS};

//     fn source_receive_address() -> String {
//         std::env::var("SOURCE_RECEIVE_ADDRESS").unwrap_or_else(|_| "127.0.0.1:9102".into())
//     }

//     #[tokio::test]
//     async fn receive_something() {
//         // TODO: This test depends on the single instance of Prometheus that we spin up for
//         // integration tests both scraping an endpoint and then also remote writing that stuff to
//         // this remote write source.  This makes sense from a "test the actual behavior" standpoint
//         // but it feels a little fragile.
//         //
//         // It could be nice to split up the Prometheus integration tests in the future, or
//         // maybe there's a way to do a one-shot remote write from Prometheus? Not sure.
//         let config = PrometheusRemoteWriteConfig {
//             address: source_receive_address().parse().unwrap(),
//             auth: None,
//             tls: None,
//             acknowledgements: AcknowledgementsConfig::default(),
//         };

//         let events = run_and_assert_source_compliance(
//             config,
//             Duration::from_secs(5),
//             &HTTP_PUSH_SOURCE_TAGS,
//         )
//         .await;
//         assert!(!events.is_empty());
//     }
// }
