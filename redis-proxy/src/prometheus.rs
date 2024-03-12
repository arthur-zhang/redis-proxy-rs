use std::net::SocketAddr;

use lazy_static::lazy_static;
use prometheus_exporter::Builder;
use prometheus_exporter::prometheus::{register_histogram_vec_with_registry, register_int_gauge_vec_with_registry, Registry};
use prometheus_exporter::prometheus::core::{AtomicI64, GenericGaugeVec};

lazy_static! {
    pub static ref REGISTRY: prometheus_exporter::prometheus::Registry =
        prometheus_exporter::prometheus::Registry::new_custom(Some("redis_proxy_rs".to_string()), None)
        .expect("metric registry should be created");
    pub static ref METRICS: Metrics = Metrics::new(REGISTRY.to_owned());
}
pub struct PrometheusServer {}

impl PrometheusServer {
    pub fn start(export_addr: &str, export_uri: &str) {
        let export_addr: SocketAddr = export_addr.parse().expect("export addr should be parsed");
        let mut exporter_builder = Builder::new(export_addr);
        exporter_builder.with_endpoint(export_uri).expect("export uri should be parsed");
        exporter_builder.with_registry(REGISTRY.to_owned());
        exporter_builder.start().expect("exporter should be started");
    }
}

pub struct Metrics {
    pub connections: GenericGaugeVec<AtomicI64>,
    pub upstream_conn_count: GenericGaugeVec<AtomicI64>,
    pub request_latency: prometheus_exporter::prometheus::HistogramVec,
}

impl Metrics {
    pub fn new(registry: Registry) -> Self {
        let connections = register_int_gauge_vec_with_registry!(
            "downstream_active", "Number of connections from client",
            &["proxy_name"], registry
        ).expect("connections metrics can be created");

        let upstream_conn_count = register_int_gauge_vec_with_registry!(
            "upstream_conn_count", "Number of connections to upstream",
            &["proxy_name"], registry
        ).expect("connections metrics can be created");

    //     let INGRESS_BYTES: Counter = register_int_counter_with_registry!(
    //     "ingress_bytes", "Total number of bytes received by the proxy",  &["proxy_name"], registry
    // ).unwrap();
        let request_latency = register_histogram_vec_with_registry!(
            "request_latency_seconds", "The latency of the requests.",
            &["proxy_name"], vec![0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0], registry
        ).expect("request_latency metrics can be created");

        Self {
            connections,
            upstream_conn_count,
            request_latency
        }
    }
}

