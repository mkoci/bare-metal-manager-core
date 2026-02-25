/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus::{CounterVec, Gauge, Opts};

use super::client::typed_value_to_string;
use super::proto::{self, PathElem};
use super::subscriber::GnmiStreamMetrics;
use crate::HealthError;
use crate::sink::{CollectorEvent, DataSink, EventContext, MetricSample};

/// We can think of the message responses like a database here, each unique ID as a "row" and all current messages comprising the "table"
type ParsedRow = HashMap<String, String>;
type TableSnapshot = HashMap<String, ParsedRow>;

pub(crate) const ON_CHANGE_STREAM_ID_SYSTEM_EVENTS: &str = "nvue_gnmi_events";

/// OnChangeStreamMetrics provides metrics surface for ON_CHANGE subscription
pub(crate) struct OnChangeStreamMetrics {
    pub(crate) rows_total: CounterVec,
    pub(crate) last_row_timestamp: Gauge,
}

impl OnChangeStreamMetrics {
    pub(crate) fn new(
        registry: &prometheus::Registry,
        prefix: &str,
        stream_id: &str,
    ) -> Result<Self, HealthError> {
        let rows_total = CounterVec::new(
            Opts::new(
                format!("{prefix}_{stream_id}_total"),
                "ON_CHANGE rows received by severity (field 'severity' if present)",
            ),
            &["switch_id", "switch_ip", "switch_mac", "severity"],
        )
        .map_err(|e| HealthError::GnmiError(format!("on_change metrics: {e}")))?;
        registry
            .register(Box::new(rows_total.clone()))
            .map_err(|e| HealthError::GnmiError(format!("register on_change rows_total: {e}")))?;

        let last_row_timestamp = Gauge::with_opts(Opts::new(
            format!("{prefix}_{stream_id}_last_timestamp"),
            "Unix timestamp of most recent ON_CHANGE row",
        ))
        .map_err(|e| HealthError::GnmiError(format!("on_change last_timestamp gauge: {e}")))?;
        registry
            .register(Box::new(last_row_timestamp.clone()))
            .map_err(|e| {
                HealthError::GnmiError(format!("register on_change last_timestamp: {e}"))
            })?;

        Ok(Self {
            rows_total,
            last_row_timestamp,
        })
    }
}

pub(crate) struct GnmiOnChangeProcessor {
    pub(crate) collector_name: String,
    pub(crate) stream_metrics: OnChangeStreamMetrics,
    pub(crate) data_sink: Option<Arc<dyn DataSink>>,
    pub(crate) event_context: EventContext,
    pub(crate) switch_id: String,
    pub(crate) switch_ip: String,
    pub(crate) switch_mac: String,
    previous_snapshot: Mutex<TableSnapshot>,
}

impl GnmiOnChangeProcessor {
    pub(crate) fn new(
        collector_name: String,
        stream_metrics: OnChangeStreamMetrics,
        data_sink: Option<Arc<dyn DataSink>>,
        event_context: EventContext,
        switch_id: String,
        switch_ip: String,
        switch_mac: String,
    ) -> Self {
        Self {
            collector_name,
            stream_metrics,
            data_sink,
            event_context,
            switch_id,
            switch_ip,
            switch_mac,
            previous_snapshot: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn process_subscribe_response(
        &self,
        resp: &proto::SubscribeResponse,
        stream_metrics: &GnmiStreamMetrics,
    ) {
        let notification = match &resp.response {
            Some(proto::subscribe_response::Response::Update(n)) => n,
            Some(proto::subscribe_response::Response::SyncResponse(_)) => return,
            Some(proto::subscribe_response::Response::Error(e)) => {
                stream_metrics.stream_errors_total.inc();
                tracing::warn!(
                    code = e.code,
                    message = %e.message,
                    stream = %self.collector_name,
                    "nvue_gnmi ON_CHANGE: server error in stream"
                );
                return;
            }
            None => return,
        };

        stream_metrics.notifications_received_total.inc();
        stream_metrics
            .last_notification_timestamp
            .set(now_unix_secs());

        let start = Instant::now();
        self.process_notification(notification);
        stream_metrics
            .notification_processing_seconds
            .observe(start.elapsed().as_secs_f64());
    }

    fn process_notification(&self, notification: &proto::Notification) {
        let prefix_elems: &[PathElem] = notification
            .prefix
            .as_ref()
            .map(|p| p.elem.as_slice())
            .unwrap_or_default();

        let mut current: TableSnapshot = HashMap::new();

        for update in &notification.update {
            let val = match update.val.as_ref() {
                Some(v) => v,
                None => continue,
            };

            let update_elems: &[PathElem] = update
                .path
                .as_ref()
                .map(|p| p.elem.as_slice())
                .unwrap_or_default();

            let combined: Vec<&PathElem> = prefix_elems.iter().chain(update_elems.iter()).collect();

            let instance_key = match find_instance_key(&combined) {
                Some(k) => k,
                None => continue,
            };

            let Some(leaf_elem) = combined.last() else {
                continue;
            };
            let value = typed_value_to_string(val).unwrap_or_default();
            current
                .entry(instance_key.to_string())
                .or_default()
                .insert(leaf_elem.name.clone(), value);
        }

        let mut prev = match self.previous_snapshot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        for (instance_id, row) in &current {
            let is_new_or_changed = prev.get(instance_id).map(|p| p != row).unwrap_or(true);
            if is_new_or_changed {
                self.emit_row_as_metric(instance_id, row);
            }
        }
        *prev = current;
    }

    fn emit_row_as_metric(&self, instance_id: &str, row: &ParsedRow) {
        let severity = row.get("severity").map(|s| s.as_str()).unwrap_or("unknown");
        let text = row.get("text").map(|s| s.as_str()).unwrap_or("");

        self.stream_metrics.last_row_timestamp.set(now_unix_secs());
        self.stream_metrics
            .rows_total
            .with_label_values(&[&self.switch_id, &self.switch_ip, &self.switch_mac, severity])
            .inc();

        tracing::info!(
            switch_id = %self.switch_id,
            stream = %self.collector_name,
            instance_id,
            severity,
            text,
            "nvue_gnmi ON_CHANGE: row received"
        );

        let Some(sink) = &self.data_sink else { return };

        let key = format!("{}:{}", self.collector_name, instance_id);
        let severity_value = severity_to_f64(Some(severity));
        let mut labels = vec![
            (Cow::Borrowed("switch_id"), self.switch_id.clone()),
            (Cow::Borrowed("switch_ip"), self.switch_ip.clone()),
            (Cow::Borrowed("switch_mac"), self.switch_mac.clone()),
            (Cow::Borrowed("instance_id"), instance_id.to_string()),
            (Cow::Borrowed("severity"), severity.to_string()),
            (Cow::Borrowed("text"), text.to_string()),
        ];
        for (k, v) in row.iter() {
            if k != "severity" && k != "text" {
                labels.push((Cow::Owned(k.clone()), v.clone()));
            }
        }

        sink.handle_event(
            &self.event_context,
            &CollectorEvent::Metric(MetricSample {
                key,
                name: self.collector_name.clone(),
                metric_type: "on_change_row".to_string(),
                unit: "severity".to_string(),
                value: severity_value,
                labels,
            }),
        );
    }
}

/// note for the reader: the NVUE gNMI ON_CHANGE response format will only ever have one keyed element.
fn find_instance_key<'a>(elems: &[&'a PathElem]) -> Option<&'a str> {
    elems
        .iter()
        .find(|e| !e.key.is_empty())
        .and_then(|e| e.key.values().next().map(String::as_str))
}

fn severity_to_f64(severity: Option<&str>) -> f64 {
    match severity {
        Some(s) if s.eq_ignore_ascii_case("informational") => 1.0,
        Some(s) if s.eq_ignore_ascii_case("warning") => 2.0,
        Some(s) if s.eq_ignore_ascii_case("error") => 3.0,
        Some(s) if s.eq_ignore_ascii_case("critical") => 4.0,
        _ => 0.0,
    }
}

fn now_unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use mac_address::MacAddress;

    use super::*;
    use crate::endpoint::BmcAddr;

    const TEST_COLLECTOR_NAME: &str = "nvue_gnmi_system_events";

    fn test_event_context(collector_type: &'static str) -> EventContext {
        EventContext {
            endpoint_key: "aa:bb:cc:dd:ee:ff".to_string(),
            addr: BmcAddr {
                ip: "10.0.0.1".parse::<IpAddr>().unwrap(),
                port: None,
                mac: MacAddress::new([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]),
            },
            collector_type,
            metadata: None,
        }
    }

    fn make_path_elem(name: &str, keys: &[(&str, &str)]) -> PathElem {
        PathElem {
            name: name.to_string(),
            key: keys
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_find_instance_key() {
        let elems = vec![
            make_path_elem("system-events", &[]),
            make_path_elem("system-event", &[("event-id", "38")]),
            make_path_elem("state", &[]),
            make_path_elem("severity", &[]),
        ];
        let refs: Vec<&PathElem> = elems.iter().collect();
        assert_eq!(find_instance_key(&refs), Some("38"));
    }

    #[test]
    fn test_find_instance_key_missing() {
        let elems = [
            make_path_elem("system-events", &[]),
            make_path_elem("state", &[]),
        ];
        let refs: Vec<&PathElem> = elems.iter().collect();
        assert_eq!(find_instance_key(&refs), None);
    }

    #[test]
    fn test_severity_to_f64() {
        assert_eq!(severity_to_f64(Some("informational")), 1.0);
        assert_eq!(severity_to_f64(Some("warning")), 2.0);
        assert_eq!(severity_to_f64(Some("error")), 3.0);
        assert_eq!(severity_to_f64(Some("critical")), 4.0);
        assert_eq!(severity_to_f64(Some("CRITICAL")), 4.0);
        assert_eq!(severity_to_f64(Some("other")), 0.0);
        assert_eq!(severity_to_f64(None), 0.0);
    }

    #[test]
    fn test_on_change_stream_metrics_new_returns_gnmi_error_on_duplicate_register() {
        let registry = prometheus::Registry::new();
        let _ = OnChangeStreamMetrics::new(&registry, "test", "stream_a").unwrap();
        let result = OnChangeStreamMetrics::new(&registry, "test", "stream_a");
        assert!(result.is_err());
        if let Err(HealthError::GnmiError(msg)) = result {
            assert!(msg.contains("register"), "expected register error: {msg}");
        } else {
            panic!("expected HealthError::GnmiError");
        }
    }

    #[test]
    fn test_process_notification_severity_and_text() {
        let registry = prometheus::Registry::new();
        let stream_metrics =
            OnChangeStreamMetrics::new(&registry, "test", TEST_COLLECTOR_NAME).unwrap();
        let processor = GnmiOnChangeProcessor::new(
            TEST_COLLECTOR_NAME.to_string(),
            stream_metrics,
            None,
            test_event_context(TEST_COLLECTOR_NAME),
            "SN1234".to_string(),
            "10.0.0.1".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        );

        let notification = proto::Notification {
            prefix: Some(proto::Path {
                elem: vec![make_path_elem("system-events", &[])],
                ..Default::default()
            }),
            update: vec![
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "5")]),
                            make_path_elem("state", &[]),
                            make_path_elem("severity", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("critical".to_string())),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "5")]),
                            make_path_elem("state", &[]),
                            make_path_elem("text", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal(
                            "System fatal state detected".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        processor.process_notification(&notification);

        let count = processor
            .stream_metrics
            .rows_total
            .with_label_values(&["SN1234", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "critical"])
            .get();
        assert_eq!(count, 1.0, "should have counted one row");
        assert!(
            processor.stream_metrics.last_row_timestamp.get() > 0.0,
            "last row timestamp should be set"
        );
    }

    #[test]
    fn test_process_notification_multiple_rows() {
        let registry = prometheus::Registry::new();
        let stream_metrics =
            OnChangeStreamMetrics::new(&registry, "test", TEST_COLLECTOR_NAME).unwrap();
        let processor = GnmiOnChangeProcessor::new(
            TEST_COLLECTOR_NAME.to_string(),
            stream_metrics,
            None,
            test_event_context(TEST_COLLECTOR_NAME),
            "SN1234".to_string(),
            "10.0.0.1".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        );

        let notification = proto::Notification {
            prefix: Some(proto::Path {
                elem: vec![make_path_elem("system-events", &[])],
                ..Default::default()
            }),
            update: vec![
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "10")]),
                            make_path_elem("state", &[]),
                            make_path_elem("severity", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("warning".to_string())),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "20")]),
                            make_path_elem("state", &[]),
                            make_path_elem("severity", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("error".to_string())),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        processor.process_notification(&notification);

        let warnings = processor
            .stream_metrics
            .rows_total
            .with_label_values(&["SN1234", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "warning"])
            .get();
        let errors = processor
            .stream_metrics
            .rows_total
            .with_label_values(&["SN1234", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "error"])
            .get();
        assert_eq!(warnings, 1.0);
        assert_eq!(errors, 1.0);
    }

    #[test]
    fn test_process_notification_all_seven_fields() {
        let registry = prometheus::Registry::new();
        let stream_metrics =
            OnChangeStreamMetrics::new(&registry, "test", TEST_COLLECTOR_NAME).unwrap();
        let processor = GnmiOnChangeProcessor::new(
            TEST_COLLECTOR_NAME.to_string(),
            stream_metrics,
            None,
            test_event_context(TEST_COLLECTOR_NAME),
            "SN1234".to_string(),
            "10.0.0.1".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        );

        let notification = proto::Notification {
            prefix: Some(proto::Path {
                elem: vec![make_path_elem("system-events", &[])],
                ..Default::default()
            }),
            update: vec![
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("severity", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("WARNING".to_string())),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("text", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal(
                            "Link down detected on swp1".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("resource", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("swp1".to_string())),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("time-created", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal(
                            "2026-02-20T15:30:00Z".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("type-id", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal(
                            "LINK_DOWN".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("state", &[]),
                            make_path_elem("event-id", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::UintVal(42)),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "42")]),
                            make_path_elem("event-id", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::UintVal(42)),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        processor.process_notification(&notification);

        let count = processor
            .stream_metrics
            .rows_total
            .with_label_values(&["SN1234", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "WARNING"])
            .get();
        assert_eq!(count, 1.0, "should have counted one row with all 7 fields");
    }

    #[test]
    fn test_process_notification_snapshot_diff_no_duplicate_emit() {
        let registry = prometheus::Registry::new();
        let stream_metrics =
            OnChangeStreamMetrics::new(&registry, "test", TEST_COLLECTOR_NAME).unwrap();
        let processor = GnmiOnChangeProcessor::new(
            TEST_COLLECTOR_NAME.to_string(),
            stream_metrics,
            None,
            test_event_context(TEST_COLLECTOR_NAME),
            "SN1234".to_string(),
            "10.0.0.1".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        );

        let notification = proto::Notification {
            prefix: Some(proto::Path {
                elem: vec![make_path_elem("system-events", &[])],
                ..Default::default()
            }),
            update: vec![
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "7")]),
                            make_path_elem("state", &[]),
                            make_path_elem("severity", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal("error".to_string())),
                    }),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("system-event", &[("event-id", "7")]),
                            make_path_elem("state", &[]),
                            make_path_elem("text", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(proto::TypedValue {
                        value: Some(proto::typed_value::Value::StringVal(
                            "same event".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        processor.process_notification(&notification);
        processor.process_notification(&notification);

        let count = processor
            .stream_metrics
            .rows_total
            .with_label_values(&["SN1234", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "error"])
            .get();
        assert_eq!(
            count, 1.0,
            "second identical full snapshot must not emit again (diff)"
        );
    }
}
