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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use prometheus::{GaugeVec, Opts};

use super::client::{typed_value_to_f64, typed_value_to_string};
use super::proto::{self, PathElem};
use super::subscriber::GnmiStreamMetrics;
use crate::HealthError;
use crate::sink::{CollectorEvent, DataSink, EventContext, MetricSample};

/// Stream ID used in gNMI subscription (metric names and collector_type).
pub(crate) const NVUE_GNMI_SAMPLE_STREAM_ID: &str = "nvue_gnmi";

/// GnmiSampleDataGauges provides metrics surface for SAMPLE subscription
pub(crate) struct GnmiSampleDataGauges {
    interface_oper_status: GaugeVec,
    interface_in_errors: GaugeVec,
    interface_out_errors: GaugeVec,
    interface_effective_ber: GaugeVec,
    interface_symbol_ber: GaugeVec,
    interface_link_down_events: GaugeVec,
    component_health_status: GaugeVec,
    component_temperature: GaugeVec,
    leak_sensor_state: GaugeVec,
}

impl GnmiSampleDataGauges {
    pub(crate) fn new(registry: &prometheus::Registry, prefix: &str) -> Result<Self, HealthError> {
        let iface_labels = &["switch_id", "switch_ip", "switch_mac", "interface_name"];
        let comp_labels = &["switch_id", "switch_ip", "switch_mac", "component_name"];
        let sensor_labels = &["switch_id", "switch_ip", "switch_mac", "sensor_id"];

        let interface_oper_status = register_gauge(
            registry,
            prefix,
            "nvue_interface_oper_status",
            "Interface operational status: 0=down, 1=up",
            iface_labels,
        )?;
        let interface_in_errors = register_gauge(
            registry,
            prefix,
            "nvue_interface_in_errors",
            "Interface inbound error counter",
            iface_labels,
        )?;
        let interface_out_errors = register_gauge(
            registry,
            prefix,
            "nvue_interface_out_errors",
            "Interface outbound error counter",
            iface_labels,
        )?;
        let interface_effective_ber = register_gauge(
            registry,
            prefix,
            "nvue_interface_effective_ber",
            "Effective bit error rate",
            iface_labels,
        )?;
        let interface_symbol_ber = register_gauge(
            registry,
            prefix,
            "nvue_interface_symbol_ber",
            "Symbol bit error rate",
            iface_labels,
        )?;
        let interface_link_down_events = register_gauge(
            registry,
            prefix,
            "nvue_interface_link_down_events",
            "Unintentional link-down events",
            iface_labels,
        )?;
        let component_health_status = register_gauge(
            registry,
            prefix,
            "nvue_component_health_status",
            "Component health: 0=unknown, 1=healthy, 2=unhealthy",
            comp_labels,
        )?;
        let component_temperature = register_gauge(
            registry,
            prefix,
            "nvue_component_temperature_celsius",
            "Component temperature in Celsius",
            comp_labels,
        )?;
        let leak_sensor_state = register_gauge(
            registry,
            prefix,
            "nvue_leak_sensor_state",
            "Leak sensor: 0=normal, 1=leak_detected",
            sensor_labels,
        )?;

        Ok(Self {
            interface_oper_status,
            interface_in_errors,
            interface_out_errors,
            interface_effective_ber,
            interface_symbol_ber,
            interface_link_down_events,
            component_health_status,
            component_temperature,
            leak_sensor_state,
        })
    }
}

fn register_gauge(
    registry: &prometheus::Registry,
    prefix: &str,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<GaugeVec, HealthError> {
    let gauge = GaugeVec::new(Opts::new(format!("{prefix}_{name}"), help), labels)?;
    registry.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// the processor contains the state needed to process NVUE gNMI SAMPLE notifications into Prometheus gauges and DataSink metric events.
pub(crate) struct GnmiSampleProcessor {
    pub(crate) data_gauges: GnmiSampleDataGauges,
    pub(crate) data_sink: Option<Arc<dyn DataSink>>,
    pub(crate) event_context: EventContext,
    pub(crate) switch_id: String,
    pub(crate) switch_ip: String,
    pub(crate) switch_mac: String,
}

impl GnmiSampleProcessor {
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
                    "nvue_gnmi SAMPLE: server error in stream"
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
        let entity_count = self.process_notification(notification);
        stream_metrics
            .notification_processing_seconds
            .observe(start.elapsed().as_secs_f64());
        stream_metrics.monitored_entities.set(entity_count as f64);
    }

    fn process_notification(&self, notification: &proto::Notification) -> usize {
        let prefix_elems: &[PathElem] = notification
            .prefix
            .as_ref()
            .map(|p| p.elem.as_slice())
            .unwrap_or_default();

        let mut entities: HashSet<(&str, &str)> = HashSet::new();

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

            if let Some(iface) = find_elem_key_ref(&combined, "interface", "name") {
                entities.insert(("interface", iface));
                self.process_interface_metric(&combined, iface, val);
            } else if let Some(comp) = find_elem_key_ref(&combined, "component", "name") {
                entities.insert(("component", comp));
                self.process_component_metric(&combined, comp, val);
            } else if let Some(sensor_id) = find_elem_key_ref(&combined, "leak-sensor", "id")
                && leaf_matches(&combined, &["state", "state"])
            {
                entities.insert(("sensor", sensor_id));
                self.process_leak_sensor_metric(val, sensor_id);
            }
        }

        entities.len()
    }

    fn process_interface_metric(
        &self,
        elems: &[&PathElem],
        iface_name: &str,
        val: &proto::TypedValue,
    ) {
        let labels = [
            &*self.switch_id,
            &*self.switch_ip,
            &*self.switch_mac,
            iface_name,
        ];

        if leaf_matches(elems, &["state", "oper-status"]) {
            let v = oper_status_to_f64(typed_value_to_string(val).as_deref());
            self.data_gauges
                .interface_oper_status
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_oper_status",
                iface_name,
                v,
                "state",
                "interface_name",
                iface_name,
            );
        } else if leaf_matches(elems, &["state", "counters", "in-errors"])
            && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .interface_in_errors
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_in_errors",
                iface_name,
                v,
                "count",
                "interface_name",
                iface_name,
            );
        } else if leaf_matches(elems, &["state", "counters", "out-errors"])
            && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .interface_out_errors
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_out_errors",
                iface_name,
                v,
                "count",
                "interface_name",
                iface_name,
            );
        } else if leaf_matches(elems, &["phy-diag", "state", "effective-ber"])
            && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .interface_effective_ber
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_effective_ber",
                iface_name,
                v,
                "ratio",
                "interface_name",
                iface_name,
            );
        } else if leaf_matches(elems, &["phy-diag", "state", "symbol-ber"])
            && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .interface_symbol_ber
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_symbol_ber",
                iface_name,
                v,
                "ratio",
                "interface_name",
                iface_name,
            );
        } else if leaf_matches(
            elems,
            &["phy-diag", "state", "unintentional-link-down-events"],
        ) && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .interface_link_down_events
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "interface_link_down_events",
                iface_name,
                v,
                "count",
                "interface_name",
                iface_name,
            );
        }
    }

    fn process_component_metric(
        &self,
        elems: &[&PathElem],
        comp_name: &str,
        val: &proto::TypedValue,
    ) {
        let labels = [
            &*self.switch_id,
            &*self.switch_ip,
            &*self.switch_mac,
            comp_name,
        ];

        if leaf_matches(elems, &["healthz", "state", "status"]) {
            let v = component_health_to_f64(typed_value_to_string(val).as_deref());
            self.data_gauges
                .component_health_status
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "component_health_status",
                comp_name,
                v,
                "state",
                "component_name",
                comp_name,
            );
        } else if leaf_matches(elems, &["state", "temperature", "instant"])
            && let Some(v) = typed_value_to_f64(val)
        {
            self.data_gauges
                .component_temperature
                .with_label_values(&labels)
                .set(v);
            self.emit_data_metric(
                "component_temperature_celsius",
                comp_name,
                v,
                "celsius",
                "component_name",
                comp_name,
            );
        }
    }

    fn process_leak_sensor_metric(&self, val: &proto::TypedValue, sensor_id: &str) {
        let v = leak_sensor_to_f64(typed_value_to_string(val).as_deref());
        let labels = [
            &*self.switch_id,
            &*self.switch_ip,
            &*self.switch_mac,
            sensor_id,
        ];
        self.data_gauges
            .leak_sensor_state
            .with_label_values(&labels)
            .set(v);
        self.emit_data_metric(
            "leak_sensor_state",
            sensor_id,
            v,
            "state",
            "sensor_id",
            sensor_id,
        );
    }

    fn emit_data_metric(
        &self,
        metric_type: &str,
        entity_id: &str,
        value: f64,
        unit: &str,
        extra_label_name: &'static str,
        extra_label_value: &str,
    ) {
        let Some(sink) = &self.data_sink else { return };

        let mut key = String::with_capacity(metric_type.len() + 1 + entity_id.len());
        key.push_str(metric_type);
        key.push(':');
        key.push_str(entity_id);

        let labels = vec![
            (Cow::Borrowed("switch_id"), self.switch_id.clone()),
            (Cow::Borrowed("switch_ip"), self.switch_ip.clone()),
            (Cow::Borrowed("switch_mac"), self.switch_mac.clone()),
            (
                Cow::Borrowed(extra_label_name),
                extra_label_value.to_string(),
            ),
        ];

        sink.handle_event(
            &self.event_context,
            &CollectorEvent::Metric(MetricSample {
                key,
                name: NVUE_GNMI_SAMPLE_STREAM_ID.to_string(),
                metric_type: metric_type.to_string(),
                unit: unit.to_string(),
                value,
                labels,
            }),
        );
    }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

fn find_elem_key_ref<'a>(
    elems: &[&'a PathElem],
    elem_name: &str,
    key_name: &str,
) -> Option<&'a str> {
    elems
        .iter()
        .find(|e| e.name == elem_name)
        .and_then(|e| e.key.get(key_name).map(String::as_str))
}

fn leaf_matches(elems: &[&PathElem], expected: &[&str]) -> bool {
    if elems.len() < expected.len() {
        return false;
    }
    let start = elems.len() - expected.len();
    elems[start..]
        .iter()
        .zip(expected)
        .all(|(elem, name)| elem.name == *name)
}

// ---------------------------------------------------------------------------
// Value mapping
// ---------------------------------------------------------------------------

fn oper_status_to_f64(status: Option<&str>) -> f64 {
    match status {
        Some(s) if s.eq_ignore_ascii_case("up") => 1.0,
        _ => 0.0,
    }
}

fn component_health_to_f64(status: Option<&str>) -> f64 {
    match status {
        Some(s) if s.eq_ignore_ascii_case("healthy") => 1.0,
        Some(s) if s.eq_ignore_ascii_case("unhealthy") => 2.0,
        _ => 0.0,
    }
}

// /platform-general/leak-sensors/leak-sensor[id=X]/state/state
// NVOS values from nvidia-platform-general-ext LeakSensors type:
//   "OK"    -> 0.0  (no leak)
//   "LEAK"  -> 1.0  (leak detected)
//   "UNSET" -> 0.0  (default / unmapped internal value)
fn leak_sensor_to_f64(status: Option<&str>) -> f64 {
    match status {
        Some(s) if s.eq_ignore_ascii_case("LEAK") => 1.0,
        _ => 0.0,
    }
}

pub(crate) fn now_unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_leaf_matches() {
        let elems: Vec<PathElem> = ["interfaces", "interface", "state", "oper-status"]
            .iter()
            .map(|n| PathElem {
                name: n.to_string(),
                key: Default::default(),
            })
            .collect();
        let refs: Vec<&PathElem> = elems.iter().collect();

        assert!(leaf_matches(&refs, &["state", "oper-status"]));
        assert!(leaf_matches(&refs, &["oper-status"]));
        assert!(!leaf_matches(&refs, &["counters", "oper-status"]));
        assert!(!leaf_matches(&refs, &["a", "b", "c", "d", "e"]));
    }

    #[test]
    fn test_find_elem_key_ref() {
        let mut key_map = HashMap::new();
        key_map.insert("name".to_string(), "nvl0".to_string());
        let elems = [
            PathElem {
                name: "interfaces".to_string(),
                key: Default::default(),
            },
            PathElem {
                name: "interface".to_string(),
                key: key_map,
            },
        ];
        let refs: Vec<&PathElem> = elems.iter().collect();

        assert_eq!(find_elem_key_ref(&refs, "interface", "name"), Some("nvl0"));
        assert_eq!(find_elem_key_ref(&refs, "interface", "id"), None);
        assert_eq!(find_elem_key_ref(&refs, "component", "name"), None);
    }

    #[test]
    fn test_oper_status_mapping() {
        assert_eq!(oper_status_to_f64(Some("UP")), 1.0);
        assert_eq!(oper_status_to_f64(Some("up")), 1.0);
        assert_eq!(oper_status_to_f64(Some("DOWN")), 0.0);
        assert_eq!(oper_status_to_f64(None), 0.0);
    }

    #[test]
    fn test_component_health_mapping() {
        assert_eq!(component_health_to_f64(Some("healthy")), 1.0);
        assert_eq!(component_health_to_f64(Some("HEALTHY")), 1.0);
        assert_eq!(component_health_to_f64(Some("unhealthy")), 2.0);
        assert_eq!(component_health_to_f64(None), 0.0);
    }

    #[test]
    fn test_leak_sensor_mapping() {
        assert_eq!(leak_sensor_to_f64(Some("OK")), 0.0);
        assert_eq!(leak_sensor_to_f64(Some("ok")), 0.0);
        assert_eq!(leak_sensor_to_f64(Some("LEAK")), 1.0);
        assert_eq!(leak_sensor_to_f64(Some("leak")), 1.0);
        assert_eq!(leak_sensor_to_f64(Some("Leak")), 1.0);
        assert_eq!(leak_sensor_to_f64(Some("UNSET")), 0.0);
        assert_eq!(leak_sensor_to_f64(None), 0.0);
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

    fn make_typed_value_string(s: &str) -> proto::TypedValue {
        proto::TypedValue {
            value: Some(proto::typed_value::Value::StringVal(s.to_string())),
        }
    }

    fn make_typed_value_uint(v: u64) -> proto::TypedValue {
        proto::TypedValue {
            value: Some(proto::typed_value::Value::UintVal(v)),
        }
    }

    fn test_processor() -> GnmiSampleProcessor {
        use std::str::FromStr;

        use mac_address::MacAddress;

        use crate::endpoint::BmcAddr;

        let registry = prometheus::Registry::new();
        let data_gauges = GnmiSampleDataGauges::new(&registry, "test").unwrap();
        let addr = BmcAddr {
            ip: "10.0.0.1".parse().unwrap(),
            port: None,
            mac: MacAddress::from_str("AA:BB:CC:DD:EE:FF").unwrap(),
        };
        let event_context = EventContext {
            endpoint_key: "aa:bb:cc:dd:ee:ff".to_string(),
            addr,
            collector_type: NVUE_GNMI_SAMPLE_STREAM_ID,
            metadata: None,
        };
        GnmiSampleProcessor {
            data_gauges,
            data_sink: None,
            event_context,
            switch_id: "serial-abc".to_string(),
            switch_ip: "10.0.0.1".to_string(),
            switch_mac: "aa:bb:cc:dd:ee:ff".to_string(),
        }
    }

    #[test]
    fn test_process_notification_interface_oper_status() {
        let proc = test_processor();
        let notification = proto::Notification {
            timestamp: 0,
            prefix: Some(proto::Path {
                elem: vec![
                    make_path_elem("interfaces", &[]),
                    make_path_elem("interface", &[("name", "nvl4")]),
                ],
                ..Default::default()
            }),
            update: vec![proto::Update {
                path: Some(proto::Path {
                    elem: vec![
                        make_path_elem("state", &[]),
                        make_path_elem("oper-status", &[]),
                    ],
                    ..Default::default()
                }),
                val: Some(make_typed_value_string("UP")),
                ..Default::default()
            }],
            ..Default::default()
        };

        let count = proc.process_notification(&notification);
        assert_eq!(count, 1);

        let gauge_val = proc
            .data_gauges
            .interface_oper_status
            .with_label_values(&["serial-abc", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "nvl4"])
            .get();
        assert_eq!(gauge_val, 1.0);
    }

    #[test]
    fn test_process_notification_component_temperature() {
        let proc = test_processor();
        let notification = proto::Notification {
            timestamp: 0,
            prefix: Some(proto::Path {
                elem: vec![
                    make_path_elem("components", &[]),
                    make_path_elem("component", &[("name", "PSU-1")]),
                ],
                ..Default::default()
            }),
            update: vec![proto::Update {
                path: Some(proto::Path {
                    elem: vec![
                        make_path_elem("state", &[]),
                        make_path_elem("temperature", &[]),
                        make_path_elem("instant", &[]),
                    ],
                    ..Default::default()
                }),
                val: Some(proto::TypedValue {
                    value: Some(proto::typed_value::Value::DoubleVal(42.5)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let count = proc.process_notification(&notification);
        assert_eq!(count, 1);

        let gauge_val = proc
            .data_gauges
            .component_temperature
            .with_label_values(&["serial-abc", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "PSU-1"])
            .get();
        assert_eq!(gauge_val, 42.5);
    }

    #[test]
    fn test_process_notification_multiple_updates() {
        let proc = test_processor();
        let notification = proto::Notification {
            timestamp: 0,
            prefix: Some(proto::Path {
                elem: vec![
                    make_path_elem("interfaces", &[]),
                    make_path_elem("interface", &[("name", "nvl0")]),
                ],
                ..Default::default()
            }),
            update: vec![
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("state", &[]),
                            make_path_elem("oper-status", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(make_typed_value_string("UP")),
                    ..Default::default()
                },
                proto::Update {
                    path: Some(proto::Path {
                        elem: vec![
                            make_path_elem("state", &[]),
                            make_path_elem("counters", &[]),
                            make_path_elem("in-errors", &[]),
                        ],
                        ..Default::default()
                    }),
                    val: Some(make_typed_value_uint(42)),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        // same interface, so entity count is 1 even with multiple updates
        let count = proc.process_notification(&notification);
        assert_eq!(count, 1);

        let labels = ["serial-abc", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "nvl0"];
        assert_eq!(
            proc.data_gauges
                .interface_oper_status
                .with_label_values(&labels)
                .get(),
            1.0
        );
        assert_eq!(
            proc.data_gauges
                .interface_in_errors
                .with_label_values(&labels)
                .get(),
            42.0
        );
    }

    #[test]
    fn test_process_notification_mixed_entities() {
        let proc = test_processor();

        let iface_update = proto::Update {
            path: Some(proto::Path {
                elem: vec![
                    make_path_elem("interfaces", &[]),
                    make_path_elem("interface", &[("name", "nvl0")]),
                    make_path_elem("state", &[]),
                    make_path_elem("oper-status", &[]),
                ],
                ..Default::default()
            }),
            val: Some(make_typed_value_string("DOWN")),
            ..Default::default()
        };

        let comp_update = proto::Update {
            path: Some(proto::Path {
                elem: vec![
                    make_path_elem("components", &[]),
                    make_path_elem("component", &[("name", "FAN-1")]),
                    make_path_elem("healthz", &[]),
                    make_path_elem("state", &[]),
                    make_path_elem("status", &[]),
                ],
                ..Default::default()
            }),
            val: Some(make_typed_value_string("healthy")),
            ..Default::default()
        };

        let notification = proto::Notification {
            timestamp: 0,
            prefix: None,
            update: vec![iface_update, comp_update],
            ..Default::default()
        };

        let count = proc.process_notification(&notification);
        assert_eq!(count, 2);

        assert_eq!(
            proc.data_gauges
                .interface_oper_status
                .with_label_values(&["serial-abc", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "nvl0"])
                .get(),
            0.0 // DOWN
        );
        assert_eq!(
            proc.data_gauges
                .component_health_status
                .with_label_values(&["serial-abc", "10.0.0.1", "aa:bb:cc:dd:ee:ff", "FAN-1"])
                .get(),
            1.0 // healthy
        );
    }
}
