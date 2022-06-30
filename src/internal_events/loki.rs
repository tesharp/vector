use metrics::counter;
use vector_core::internal_event::InternalEvent;

use super::prelude::{error_stage, error_type};

#[derive(Debug)]
pub struct LokiEventUnlabeled;

impl InternalEvent for LokiEventUnlabeled {
    fn emit(self) {
        counter!("processing_errors_total", 1,
                "error_type" => "unlabeled_event");
    }
}

#[derive(Debug)]
pub struct LokiOutOfOrderEventDropped {
    pub count: usize,
}

impl InternalEvent for LokiOutOfOrderEventDropped {
    fn emit(self) {
        debug!(
            message = "Received out-of-order events; dropping events.",
            count = %self.count,
            internal_log_rate_secs = 10,
        );
        counter!("events_discarded_total", self.count as u64,
                "reason" => "out_of_order"); // deprecated
        counter!("processing_errors_total", 1,
                "error_type" => "out_of_order"); // deprecated
        counter!("component_discarded_events_total", self.count as u64,
                "reason" => "out_of_order");
    }
}

#[derive(Debug)]
pub struct LokiOutOfOrderEventRewritten {
    pub count: usize,
}

impl InternalEvent for LokiOutOfOrderEventRewritten {
    fn emit(self) {
        debug!(
            message = "Received out-of-order events, rewriting timestamps.",
            count = %self.count,
            internal_log_rate_secs = 10,
        );
        counter!("processing_errors_total", 1,
                "error_type" => "out_of_order"); // deprecated
        counter!("rewritten_timestamp_events_total", self.count as u64);
    }
}

pub struct LokiParseError {
    pub error: prost::DecodeError,
}

impl InternalEvent for LokiParseError {
    fn emit(self) {
        error!(
            message = "Parsing error.",
            error = %self.error,
            error_code = "parser_error",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,
        );

        counter!(
            "component_errors_total", 1,
            "error_code" => "parser_error",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        );
    }
}
