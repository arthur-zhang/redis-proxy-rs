use std::net::SocketAddr;

use lazy_static::lazy_static;
use prometheus_exporter::Builder;
use prometheus_exporter::prometheus::{HistogramVec, register_histogram_vec_with_registry, register_int_counter_vec_with_registry};
use prometheus_exporter::prometheus::core::{AtomicI64, AtomicU64, GenericCounterVec, GenericGaugeVec};
use prometheus_exporter::prometheus::register_int_gauge_vec_with_registry;
use prometheus_exporter::prometheus::Registry;

pub const CONN_DOWNSTREAM: &str = "downstream";
pub const CONN_UPSTREAM: &str = "upstream";

pub const TRAFFIC_TYPE_INGRESS: &str = "ingress";
pub const TRAFFIC_TYPE_EGRESS: &str = "egress";

pub const RESP_SUCCESS: &str = "success";
pub const RESP_FAILED: &str = "failed";

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
    pub request_latency: HistogramVec,
    pub bandwidth: GenericCounterVec<AtomicU64>,
}

impl Metrics {
    pub fn new(registry: Registry) -> Self {
        let connections = register_int_gauge_vec_with_registry!(
            "connections", "Number of connections from downstream or upstream",
            &["type"], registry
        ).expect("connections metrics can be created");

        let request_latency = register_histogram_vec_with_registry!(
            "request_latency", "The latency of the requests.",
            &["cmd", "status"], vec![0.001, 0.002, 0.005, 0.01, 0.1, 1.0, 10.0], registry
        ).expect("request_latency metrics can be created");

        let bandwidth = register_int_counter_vec_with_registry!(
            "bandwidth", "The bandwidth of the requests.",
            &["cmd", "type"], registry
        ).expect("bandwidth metrics can be created");
        
        Self {
            connections,
            request_latency,
            bandwidth
        }
    }
}

