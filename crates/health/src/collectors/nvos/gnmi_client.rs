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

use std::time::Duration;

use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use super::gnmi::proto::g_nmi_client::GNmiClient as TonicGnmiClient;
use super::gnmi::proto::subscription_list::Mode as SubscriptionListMode;
use super::gnmi::proto::{
    self, Encoding, Path, PathElem, SubscribeRequest, Subscription, SubscriptionList,
    SubscriptionMode,
};
use crate::HealthError;

/// The currently targeted gNMI subscription paths for critical health metrics.
pub fn nvos_subscribe_paths() -> Vec<Path> {
    vec![
        build_path(&[("components", None), ("component", None)]),
        build_path(&[("interfaces", None), ("interface", None)]),
        build_path(&[("platform-general", None), ("leak-sensors", None)]),
        build_path(&[("system", None), ("health", None)]),
    ]
}

pub struct GnmiClient {
    switch_id: String,
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    request_timeout: Duration,
}

impl GnmiClient {
    pub fn new(
        switch_id: String,
        host: &str,
        port: u16,
        username: Option<String>,
        password: Option<String>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            switch_id,
            host: host.to_string(),
            port,
            username,
            password,
            request_timeout,
        }
    }

    async fn connect(&self) -> Result<TonicGnmiClient<Channel>, HealthError> {
        let target = format!("{}:{}", self.host, self.port);
        let uri = format!("https://{target}");

        let endpoint = Endpoint::from_shared(uri)
            .map_err(|e| {
                HealthError::GnmiError(format!("switch {}: invalid endpoint: {e}", self.switch_id))
            })?
            .connect_timeout(self.request_timeout)
            .timeout(self.request_timeout);

        let tls_config = super::tls::self_signed_tls_config();
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_only()
            .enable_http2()
            .build();

        let channel = endpoint
            .connect_with_connector(connector)
            .await
            .map_err(|e| {
                HealthError::GnmiError(format!(
                    "switch {}: connection failed to {target}: {e}",
                    self.switch_id
                ))
            })?;

        tracing::debug!(
            switch_id = %self.switch_id,
            target = %target,
            "gNMI TLS channel established (skip-verify)"
        );

        Ok(TonicGnmiClient::new(channel))
    }

    /// Perform a gNMI ONCE subscribe for the given paths, collecting all
    /// notifications until the server sends `sync_response = true`.
    pub async fn subscribe_once(
        &self,
        paths: Vec<Path>,
    ) -> Result<Vec<proto::Notification>, HealthError> {
        let mut client = self.connect().await?;

        let subscription_list = SubscriptionList {
            prefix: Some(Path {
                target: "nvos".to_string(),
                ..Default::default()
            }),
            subscription: paths
                .into_iter()
                .map(|path| Subscription {
                    path: Some(path),
                    mode: SubscriptionMode::TargetDefined.into(),
                    ..Default::default()
                })
                .collect(),
            mode: SubscriptionListMode::Once.into(),
            encoding: Encoding::Json.into(),
            ..Default::default()
        };

        let subscribe_request = SubscribeRequest {
            request: Some(proto::subscribe_request::Request::Subscribe(
                subscription_list,
            )),
            extension: vec![],
        };

        let stream = tokio_stream::once(subscribe_request);
        let mut request = Request::new(stream);
        add_auth_metadata(&mut request, &self.username, &self.password);

        let response = client.subscribe(request).await.map_err(|e| {
            HealthError::GnmiError(format!(
                "switch {}: subscribe RPC failed: {e}",
                self.switch_id
            ))
        })?;

        let mut stream = response.into_inner();
        let mut notifications = Vec::new();

        loop {
            match stream.message().await {
                Ok(Some(resp)) => match resp.response {
                    Some(proto::subscribe_response::Response::Update(notification)) => {
                        notifications.push(notification);
                    }
                    Some(proto::subscribe_response::Response::SyncResponse(true)) => break,
                    Some(proto::subscribe_response::Response::SyncResponse(false)) => continue,
                    Some(proto::subscribe_response::Response::Error(e)) => {
                        return Err(HealthError::GnmiError(format!(
                            "switch {}: server error: {} (code {})",
                            self.switch_id, e.message, e.code
                        )));
                    }
                    None => continue,
                },
                Ok(None) => break,
                Err(e) => {
                    return Err(HealthError::GnmiError(format!(
                        "switch {}: stream error: {e}",
                        self.switch_id
                    )));
                }
            }
        }

        tracing::debug!(
            switch_id = %self.switch_id,
            notification_count = notifications.len(),
            "gNMI ONCE subscribe complete"
        );

        Ok(notifications)
    }
}

fn add_auth_metadata<T>(
    request: &mut Request<T>,
    username: &Option<String>,
    password: &Option<String>,
) {
    if let Some(username) = username {
        if let Ok(value) = username.parse() {
            request.metadata_mut().insert("username", value);
        } else {
            tracing::warn!("invalid username for gRPC metadata, skipping");
        }
    }
    if let Some(password) = password {
        if let Ok(value) = password.parse() {
            request.metadata_mut().insert("password", value);
        } else {
            tracing::warn!("invalid password for gRPC metadata, skipping");
        }
    }
}

// ---------------------------------------------------------------------------
// Path construction helpers
// ---------------------------------------------------------------------------

/// Build a gNMI `Path` from a slice of `(element_name, optional_key)` tuples.
pub fn build_path(elements: &[(&str, Option<(&str, &str)>)]) -> Path {
    Path {
        elem: elements
            .iter()
            .map(|(name, key)| PathElem {
                name: name.to_string(),
                key: key
                    .map(|(k, v)| std::iter::once((k.to_string(), v.to_string())).collect())
                    .unwrap_or_default(),
            })
            .collect(),
        ..Default::default()
    }
}

/// Format a `Path` as a human-readable slash-separated string with keys.
pub fn path_to_string(path: &Path) -> String {
    if path.elem.is_empty() {
        return "/".to_string();
    }
    let mut result = String::new();
    for elem in &path.elem {
        result.push('/');
        result.push_str(&elem.name);
        for (k, v) in &elem.key {
            result.push_str(&format!("[{k}={v}]"));
        }
    }
    result
}

/// Extract a key value from a specific path element.
pub fn extract_key_from_path(path: &Path, elem_name: &str, key_name: &str) -> Option<String> {
    path.elem
        .iter()
        .find(|elem| elem.name == elem_name)
        .and_then(|elem| elem.key.get(key_name).cloned())
}

// ---------------------------------------------------------------------------
// Notification value extraction helpers
// ---------------------------------------------------------------------------

/// Extract a string from a `TypedValue`, handling JSON-encoded bytes as well
/// as native string values.
pub fn typed_value_to_string(val: &proto::TypedValue) -> Option<String> {
    use proto::typed_value::Value;
    match &val.value {
        Some(Value::StringVal(s)) => Some(s.clone()),
        Some(Value::JsonVal(bytes)) | Some(Value::JsonIetfVal(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            let trimmed = s.trim().trim_matches('"');
            Some(trimmed.to_string())
        }
        Some(Value::AsciiVal(s)) => Some(s.clone()),
        Some(Value::IntVal(v)) => Some(v.to_string()),
        Some(Value::UintVal(v)) => Some(v.to_string()),
        Some(Value::BoolVal(v)) => Some(v.to_string()),
        Some(Value::FloatVal(v)) => Some(v.to_string()),
        Some(Value::DoubleVal(v)) => Some(v.to_string()),
        _ => None,
    }
}

/// Extract a float from a `TypedValue`, handling JSON-encoded bytes, native
/// numeric values, and string representations.
pub fn typed_value_to_f64(val: &proto::TypedValue) -> Option<f64> {
    use proto::typed_value::Value;
    match &val.value {
        Some(Value::DoubleVal(v)) => Some(*v),
        Some(Value::FloatVal(v)) => Some(*v as f64),
        Some(Value::IntVal(v)) => Some(*v as f64),
        Some(Value::UintVal(v)) => Some(*v as f64),
        Some(Value::StringVal(s)) => s.parse().ok(),
        Some(Value::JsonVal(bytes)) | Some(Value::JsonIetfVal(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            s.trim().trim_matches('"').parse().ok()
        }
        _ => None,
    }
}

/// Extract an unsigned integer from a `TypedValue`.
pub fn typed_value_to_u64(val: &proto::TypedValue) -> Option<u64> {
    use proto::typed_value::Value;
    match &val.value {
        Some(Value::UintVal(v)) => Some(*v),
        Some(Value::IntVal(v)) => u64::try_from(*v).ok(),
        Some(Value::DoubleVal(v)) => Some(*v as u64),
        Some(Value::FloatVal(v)) => Some(*v as u64),
        Some(Value::StringVal(s)) => s.parse().ok(),
        Some(Value::JsonVal(bytes)) | Some(Value::JsonIetfVal(bytes)) => {
            let s = String::from_utf8_lossy(bytes);
            s.trim().trim_matches('"').parse().ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_path_no_keys() {
        let path = build_path(&[("system", None), ("health", None)]);
        assert_eq!(path.elem.len(), 2);
        assert_eq!(path.elem[0].name, "system");
        assert!(path.elem[0].key.is_empty());
        assert_eq!(path.elem[1].name, "health");
        assert!(path.elem[1].key.is_empty());
    }

    #[test]
    fn test_build_path_with_keys() {
        let path = build_path(&[("components", None), ("component", Some(("name", "PSU-1")))]);
        assert_eq!(path.elem.len(), 2);
        assert_eq!(path.elem[0].name, "components");
        assert!(path.elem[0].key.is_empty());
        assert_eq!(path.elem[1].name, "component");
        assert_eq!(path.elem[1].key.get("name").unwrap(), "PSU-1");
    }

    #[test]
    fn test_build_path_empty() {
        let path = build_path(&[]);
        assert!(path.elem.is_empty());
    }

    #[test]
    fn test_path_to_string_simple() {
        let path = build_path(&[("system", None), ("health", None)]);
        assert_eq!(path_to_string(&path), "/system/health");
    }

    #[test]
    fn test_path_to_string_with_keys() {
        let path = build_path(&[("interfaces", None), ("interface", Some(("name", "nvl0")))]);
        assert_eq!(path_to_string(&path), "/interfaces/interface[name=nvl0]");
    }

    #[test]
    fn test_path_to_string_empty() {
        let path = build_path(&[]);
        assert_eq!(path_to_string(&path), "/");
    }

    #[test]
    fn test_extract_key_found() {
        let path = build_path(&[("components", None), ("component", Some(("name", "FAN-3")))]);
        assert_eq!(
            extract_key_from_path(&path, "component", "name"),
            Some("FAN-3".to_string())
        );
    }

    #[test]
    fn test_extract_key_wrong_elem() {
        let path = build_path(&[("components", None), ("component", Some(("name", "FAN-3")))]);
        assert_eq!(extract_key_from_path(&path, "components", "name"), None);
    }

    #[test]
    fn test_extract_key_wrong_key() {
        let path = build_path(&[("components", None), ("component", Some(("name", "FAN-3")))]);
        assert_eq!(extract_key_from_path(&path, "component", "id"), None);
    }

    #[test]
    fn test_typed_value_to_string_string_val() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::StringVal("healthy".to_string())),
        };
        assert_eq!(typed_value_to_string(&val), Some("healthy".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_json_val() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::JsonVal(b"\"degraded\"".to_vec())),
        };
        assert_eq!(typed_value_to_string(&val), Some("degraded".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_json_unquoted() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::JsonVal(b"42".to_vec())),
        };
        assert_eq!(typed_value_to_string(&val), Some("42".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_int() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::IntVal(-5)),
        };
        assert_eq!(typed_value_to_string(&val), Some("-5".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_uint() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::UintVal(100)),
        };
        assert_eq!(typed_value_to_string(&val), Some("100".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_bool() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::BoolVal(true)),
        };
        assert_eq!(typed_value_to_string(&val), Some("true".to_string()));
    }

    #[test]
    fn test_typed_value_to_string_none() {
        let val = proto::TypedValue { value: None };
        assert_eq!(typed_value_to_string(&val), None);
    }

    #[test]
    fn test_typed_value_to_f64_double() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::DoubleVal(42.5)),
        };
        assert_eq!(typed_value_to_f64(&val), Some(42.5));
    }

    #[test]
    fn test_typed_value_to_f64_int() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::IntVal(42)),
        };
        assert_eq!(typed_value_to_f64(&val), Some(42.0));
    }

    #[test]
    fn test_typed_value_to_f64_json_string() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::JsonVal(b"\"1.5e-3\"".to_vec())),
        };
        assert_eq!(typed_value_to_f64(&val), Some(0.0015));
    }

    #[test]
    fn test_typed_value_to_f64_json_number() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::JsonVal(b"99.9".to_vec())),
        };
        assert_eq!(typed_value_to_f64(&val), Some(99.9));
    }

    #[test]
    fn test_typed_value_to_f64_string() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::StringVal("1.23".to_string())),
        };
        assert_eq!(typed_value_to_f64(&val), Some(1.23));
    }

    #[test]
    fn test_typed_value_to_f64_non_numeric_string() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::StringVal("hello".to_string())),
        };
        assert_eq!(typed_value_to_f64(&val), None);
    }

    #[test]
    fn test_typed_value_to_f64_none() {
        let val = proto::TypedValue { value: None };
        assert_eq!(typed_value_to_f64(&val), None);
    }

    #[test]
    fn test_typed_value_to_u64_uint() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::UintVal(1000)),
        };
        assert_eq!(typed_value_to_u64(&val), Some(1000));
    }

    #[test]
    fn test_typed_value_to_u64_int_positive() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::IntVal(50)),
        };
        assert_eq!(typed_value_to_u64(&val), Some(50));
    }

    #[test]
    fn test_typed_value_to_u64_int_negative() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::IntVal(-1)),
        };
        assert_eq!(typed_value_to_u64(&val), None);
    }

    #[test]
    fn test_typed_value_to_u64_json() {
        let val = proto::TypedValue {
            value: Some(proto::typed_value::Value::JsonVal(b"\"512\"".to_vec())),
        };
        assert_eq!(typed_value_to_u64(&val), Some(512));
    }

    #[test]
    fn test_typed_value_to_u64_none() {
        let val = proto::TypedValue { value: None };
        assert_eq!(typed_value_to_u64(&val), None);
    }

    #[test]
    fn test_nvos_subscribe_paths() {
        let paths = nvos_subscribe_paths();
        assert_eq!(paths.len(), 4);

        assert_eq!(path_to_string(&paths[0]), "/components/component");
        assert_eq!(path_to_string(&paths[1]), "/interfaces/interface");
        assert_eq!(path_to_string(&paths[2]), "/platform-general/leak-sensors");
        assert_eq!(path_to_string(&paths[3]), "/system/health");
    }
}
