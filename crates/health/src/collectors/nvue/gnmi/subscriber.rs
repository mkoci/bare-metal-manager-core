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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use prometheus::{Counter, Gauge, Histogram, HistogramOpts, Opts};
use tokio_util::sync::CancellationToken;

use super::client::{GnmiClient, nvue_subscribe_paths};
use super::processor::{COLLECTOR_NAME, GnmiDataGauges, GnmiProcessor, now_unix_secs};
use super::proto;
use crate::HealthError;
use crate::collectors::Collector;
use crate::collectors::runtime::ExponentialBackoff;
use crate::config::NvueCollectorConfig;
use crate::endpoint::{BmcEndpoint, EndpointMetadata};
use crate::metrics::CollectorRegistry;
use crate::sink::{DataSink, EventContext};

// gRPC ConnectivityState mapping for the connection_state gauge.
// ref: https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md
//
// 0 = UNKNOWN  -- prometheus gauge default
// 1 = IDLE     -- server closed stream cleanly
// 2 = CONNECTING
// 3 = READY    -- subscription live and processing notifications
// 4 = TRANSIENT_FAILURE -- connection/stream error
// 5 = SHUTDOWN -- CancellationToken fired and task exiting
const IDLE: f64 = 1.0;
const CONNECTING: f64 = 2.0;
const READY: f64 = 3.0;
const TRANSIENT_FAILURE: f64 = 4.0;
const SHUTDOWN: f64 = 5.0;

// ---------------------------------------------------------------------------
// GnmiStreamMetrics -- connection health metrics
// ---------------------------------------------------------------------------

pub(crate) struct GnmiStreamMetrics {
    pub(crate) connection_state: Gauge,
    pub(crate) reconnections_total: Counter,
    pub(crate) connection_established_timestamp: Gauge,
    pub(crate) notifications_received_total: Counter,
    pub(crate) last_notification_timestamp: Gauge,
    pub(crate) notification_processing_seconds: Histogram,
    pub(crate) stream_errors_total: Counter,
    pub(crate) monitored_entities: Gauge,
}

impl GnmiStreamMetrics {
    fn new(
        registry: &prometheus::Registry,
        prefix: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Self, HealthError> {
        let connection_state = Gauge::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_connection_state"),
                "gRPC connection state: 0=UNKNOWN, 1=IDLE, 2=CONNECTING, 3=READY, 4=TRANSIENT_FAILURE, 5=SHUTDOWN",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(connection_state.clone()))?;

        let reconnections_total = Counter::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_reconnections_total"),
                "Total reconnection attempts",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(reconnections_total.clone()))?;

        let connection_established_timestamp = Gauge::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_connection_established_timestamp"),
                "Unix timestamp when current connection was established. Compute uptime via time() - this_metric.",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(connection_established_timestamp.clone()))?;

        let notifications_received_total = Counter::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_notifications_received_total"),
                "Total notification messages received",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(notifications_received_total.clone()))?;

        let last_notification_timestamp = Gauge::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_last_notification_timestamp"),
                "Unix timestamp of most recent notification",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(last_notification_timestamp.clone()))?;

        let notification_processing_seconds = Histogram::with_opts(
            HistogramOpts::new(
                format!("{prefix}_nvue_gnmi_notification_processing_seconds"),
                "Per-notification processing time",
            )
            .const_labels(const_labels.clone())
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        )?;
        registry.register(Box::new(notification_processing_seconds.clone()))?;

        let stream_errors_total = Counter::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_stream_errors_total"),
                "Total stream errors",
            )
            .const_labels(const_labels.clone()),
        )?;
        registry.register(Box::new(stream_errors_total.clone()))?;

        let monitored_entities = Gauge::with_opts(
            Opts::new(
                format!("{prefix}_nvue_gnmi_monitored_entities"),
                "Unique entities in most recent notification batch",
            )
            .const_labels(const_labels),
        )?;
        registry.register(Box::new(monitored_entities.clone()))?;

        Ok(Self {
            connection_state,
            reconnections_total,
            connection_established_timestamp,
            notifications_received_total,
            last_notification_timestamp,
            notification_processing_seconds,
            stream_errors_total,
            monitored_entities,
        })
    }
}

// ---------------------------------------------------------------------------
// GnmiStreamConfig -- connection parameters
// ---------------------------------------------------------------------------

struct GnmiStreamConfig {
    client: GnmiClient,
    paths: Vec<proto::Path>,
    sample_interval_nanos: u64,
}

// ---------------------------------------------------------------------------
// Public spawn function
// ---------------------------------------------------------------------------

pub fn spawn_gnmi_collector(
    endpoint: &BmcEndpoint,
    nvue_config: &NvueCollectorConfig,
    collector_registry: Arc<CollectorRegistry>,
    data_sink: Option<Arc<dyn DataSink>>,
) -> Result<Collector, HealthError> {
    let switch_id = match &endpoint.metadata {
        Some(EndpointMetadata::Switch(s)) => s.serial.clone(),
        _ => endpoint.addr.mac.to_string(),
    };
    let switch_ip = endpoint.addr.ip.to_string();
    let switch_mac = endpoint.addr.mac.to_string();
    let event_context = EventContext::from_endpoint(endpoint, COLLECTOR_NAME);

    let client = GnmiClient::new(
        switch_id.clone(),
        &switch_ip,
        nvue_config.gnmi_port,
        Some(endpoint.credentials.username.clone()),
        Some(endpoint.credentials.password.clone()),
        nvue_config.request_timeout,
    );

    let registry = collector_registry.registry();
    let prefix = collector_registry.prefix().clone();

    let const_labels = HashMap::from([
        ("collector_type".to_string(), COLLECTOR_NAME.to_string()),
        ("switch_id".to_string(), switch_id.clone()),
        ("switch_ip".to_string(), switch_ip.clone()),
        ("switch_mac".to_string(), switch_mac.clone()),
    ]);

    let stream_metrics = GnmiStreamMetrics::new(registry, &prefix, const_labels)?;
    let data_gauges = GnmiDataGauges::new(registry, &prefix)?;

    let stream_config = GnmiStreamConfig {
        client,
        paths: nvue_subscribe_paths(),
        sample_interval_nanos: nvue_config.poll_interval.as_nanos() as u64,
    };

    let processor = GnmiProcessor {
        data_gauges,
        data_sink,
        event_context,
        switch_id,
        switch_ip,
        switch_mac,
    };

    Ok(Collector::spawn_task(move |cancel_token| {
        gnmi_stream_task(
            cancel_token,
            stream_config,
            stream_metrics,
            processor,
            collector_registry,
        )
    }))
}

// ---------------------------------------------------------------------------
// Streaming task
// ---------------------------------------------------------------------------

async fn gnmi_stream_task(
    cancel_token: CancellationToken,
    config: GnmiStreamConfig,
    stream_metrics: GnmiStreamMetrics,
    processor: GnmiProcessor,
    _collector_registry: Arc<CollectorRegistry>,
) {
    let mut backoff = ExponentialBackoff::new(Duration::from_secs(2), Duration::from_secs(60));

    loop {
        stream_metrics.connection_state.set(CONNECTING);

        let stream = tokio::select! {
            _ = cancel_token.cancelled() => return,
            result = config.client.subscribe_sample(&config.paths, config.sample_interval_nanos) => result,
        };

        let mut stream = match stream {
            Ok(s) => {
                stream_metrics.connection_state.set(READY);
                stream_metrics
                    .connection_established_timestamp
                    .set(now_unix_secs());
                backoff.reset();
                tracing::info!(
                    switch_id = %processor.switch_id,
                    "nvue_gnmi: stream connected"
                );
                s
            }
            Err(e) => {
                stream_metrics.connection_state.set(TRANSIENT_FAILURE);
                stream_metrics.reconnections_total.inc();
                tracing::warn!(
                    error = ?e,
                    switch_id = %processor.switch_id,
                    "nvue_gnmi: connection failed, backing off"
                );
                tokio::select! {
                    _ = cancel_token.cancelled() => return,
                    _ = tokio::time::sleep(backoff.next_delay()) => continue,
                };
            }
        };

        loop {
            let msg = tokio::select! {
                _ = cancel_token.cancelled() => {
                    stream_metrics.connection_state.set(SHUTDOWN);
                    tracing::info!(
                        switch_id = %processor.switch_id,
                        "nvue_gnmi: cancelled, shutting down"
                    );
                    return;
                }
                msg = stream.message() => msg,
            };

            match msg {
                Ok(Some(resp)) => {
                    processor.process_subscribe_response(&resp, &stream_metrics);
                }
                Ok(None) => {
                    stream_metrics.connection_state.set(IDLE);
                    stream_metrics.reconnections_total.inc();
                    tracing::info!(
                        switch_id = %processor.switch_id,
                        "nvue_gnmi: stream closed by server, reconnecting"
                    );
                    break;
                }
                Err(e) => {
                    stream_metrics.connection_state.set(TRANSIENT_FAILURE);
                    stream_metrics.stream_errors_total.inc();
                    stream_metrics.reconnections_total.inc();
                    tracing::warn!(
                        error = ?e,
                        switch_id = %processor.switch_id,
                        "nvue_gnmi: stream error, reconnecting"
                    );
                    break;
                }
            }
        }

        tokio::select! {
            _ = cancel_token.cancelled() => return,
            _ = tokio::time::sleep(backoff.next_delay()) => {},
        };
    }
}
