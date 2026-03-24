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

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::processor::EventProcessingPipeline;
use crate::sink::{CollectorEvent, DataSink, EventContext};

/// sync pipeline + optional bounded OTLP channel. the channel send is the
/// backpressure point; sync sinks always complete before it.
pub struct EventPipeline {
    inner: Arc<EventProcessingPipeline>,
    otlp_sender: Option<mpsc::Sender<(EventContext, CollectorEvent)>>,
}

impl EventPipeline {
    pub fn new(
        inner: EventProcessingPipeline,
        otlp_sender: Option<mpsc::Sender<(EventContext, CollectorEvent)>>,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            otlp_sender,
        }
    }

    pub async fn handle_event(&self, context: &EventContext, event: &CollectorEvent) {
        let all_events = self.inner.handle_and_collect(context, event);

        if let Some(sender) = &self.otlp_sender {
            for evt in all_events {
                if sender.send((context.clone(), evt)).await.is_err() {
                    tracing::warn!("otlp channel closed, event dropped");
                    break;
                }
            }
        }
    }

    pub fn as_data_sink(&self) -> Arc<dyn DataSink> {
        Arc::clone(&self.inner) as Arc<dyn DataSink>
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mac_address::MacAddress;

    use super::*;
    use crate::endpoint::BmcAddr;
    use crate::sink::DataSink;

    struct CountingSink {
        counter: Arc<AtomicUsize>,
    }

    impl DataSink for CountingSink {
        fn handle_event(&self, _: &EventContext, _: &CollectorEvent) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn test_context() -> EventContext {
        EventContext {
            endpoint_key: "42:9e:b1:bd:9d:dd".to_string(),
            addr: BmcAddr {
                ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                port: Some(443),
                mac: MacAddress::from_str("42:9e:b1:bd:9d:dd").expect("valid mac"),
            },
            collector_type: "test",
            metadata: None,
        }
    }

    #[tokio::test]
    async fn sync_sinks_complete_before_otlp_send() {
        let sink_counter = Arc::new(AtomicUsize::new(0));
        let inner = EventProcessingPipeline::new(
            vec![],
            vec![Arc::new(CountingSink {
                counter: sink_counter.clone(),
            })],
        );
        let (otlp_tx, mut otlp_rx) = tokio::sync::mpsc::channel(10);
        let pipeline = EventPipeline::new(inner, Some(otlp_tx));

        let context = test_context();
        let event = CollectorEvent::MetricCollectionStart;
        pipeline.handle_event(&context, &event).await;

        assert_eq!(sink_counter.load(Ordering::SeqCst), 1);
        let received = otlp_rx.try_recv();
        assert!(received.is_ok());
    }

    #[tokio::test]
    async fn works_without_otlp_sender() {
        let sink_counter = Arc::new(AtomicUsize::new(0));
        let inner = EventProcessingPipeline::new(
            vec![],
            vec![Arc::new(CountingSink {
                counter: sink_counter.clone(),
            })],
        );
        let pipeline = EventPipeline::new(inner, None);

        let context = test_context();
        let event = CollectorEvent::MetricCollectionStart;
        pipeline.handle_event(&context, &event).await;

        assert_eq!(sink_counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn backpressure_suspends_on_full_channel() {
        let inner = EventProcessingPipeline::new(vec![], vec![]);
        let (otlp_tx, _otlp_rx) = tokio::sync::mpsc::channel(1);
        let pipeline = Arc::new(EventPipeline::new(inner, Some(otlp_tx)));

        let context = test_context();
        let event = CollectorEvent::MetricCollectionStart;

        // first send fills the channel
        pipeline.handle_event(&context, &event).await;

        // second send should not complete within the timeout (channel full)
        let pipeline_clone = Arc::clone(&pipeline);
        let ctx = context.clone();
        let evt = event.clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            pipeline_clone.handle_event(&ctx, &evt),
        )
        .await;

        assert!(result.is_err(), "expected timeout due to full channel");
    }
}
