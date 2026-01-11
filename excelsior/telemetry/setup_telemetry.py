import asyncio
import logging
import os
from functools import wraps


from opentelemetry import metrics
from opentelemetry import trace
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAMESPACE

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider

try:
    trace_provider = otel.get_trace_provider(
        "excelsior"
    )

    metric_provider = otel.get_meter_provider(
        "excelsior",
    )
except Exception as e:
    logging.warning(f"Telemetry initialization failed: {str(e)}. Running without telemetry.")
    trace_provider = TracerProvider()
    meter_provider = MeterProvider()




class UselessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return (
            message.find("/health/readiness") == -1
            and message.find("/health/liveness") == -1
            and message.find("POST /v1/logs HTTP/11") == -1
                and message.find("POST /v1/metrics HTTP/11") == -1
        )


def traceFunction(username=None):
    def _decorator(func):

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return _trace_logic(func, username, *args, **kwargs)
            except Exception as e:
                logging.warning(f"Telemetry wrapper failed: {e}. Executing function without telemetry.")
                return func(*args, **kwargs)
            

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await _async_trace_logic(func, username, *args, **kwargs)
            except Exception as e:
                logging.warning(f"Telemetry wrapper failed: {e}. Executing function without telemetry.")
                return func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return _decorator() if callable(username) else _decorator


def _trace_logic(func, username, *args, **kwargs):
    tracer = trace_provider.get_tracer(__name__)
    trace_name = f"function name: {func.__name__}"

    try:
        with tracer.start_as_current_span(trace_name) as span:
            try:
                span.set_attribute("function_name", func.__name__)
                span.set_attribute("one_bank_id", username)
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logging.warning(f"Failed to set span attributes: {e}")
    except Exception as e:
        logging.warning(f"Tracing failed: {e}. Executing function without tracing.")
        return func(*args, **kwargs)


async def _async_trace_logic(func, username, *args, **kwargs):
    tracer = trace_provider.get_tracer(__name__)

    trace_name = f"function name: {func.__name__}"

    try:    
        with tracer.start_as_current_span(trace_name) as span:
            try:
                span.set_attribute("function_name", func.__name__)
                span.set_attribute("one_bank_id", username)
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                    logging.warning(f"Failed to set span attributes: {e}")
    except Exception as e:
        logging.warning(f"Tracing failed: {e}. Executing function without tracing.")
        return func(*args, **kwargs)

def setupTelemetry():
    try:
        # metrics.set_meter_provider(metric_provider)
        trace.set_tracer_provider(trace_provider)
        LoggingInstrumentor().instrument(set_logging_format=True)
        RequestsInstrumentor().instrument()
    except Exception as e:
        logging.warning(f"Failed to set meter and tracer provider: {e}")



def setupLogging():
    setupTelemetry()
    otel_handler = otel.get_logging_handler("excelsior", level=logging.INFO)
    visitLogger = logging.getLogger("urllib3.connectionpool")
    visitLogger.addFilter(UselessLogFilter())
    otel_handler.addFilter(UselessLogFilter())
    root_logger = logging.getLogger()
    root_logger.addHandler(otel_handler)

    logging.getLogger("uvicorn").addHandler(otel_handler)
    logging.getLogger("uvicorn.access").addHandler(otel_handler)
    logging.getLogger("uvicorn.error").addHandler(otel_handler)

    class OpenTelemetryFilter(logging.Filter):
        def filter(self, record):
            from opentelemetry import trace

            span = trace.get_current_span()
            if span:
                context = span.get_span_context()
                record.otelTraceID = trace.format_trace_id(context.trace_id)
                record.otelSpanID = trace.format_span_id(context.span_id)
            else:
                record.otelTraceID = record.otelSpanID = None
            return True

    logging.getLogger().addFilter(OpenTelemetryFilter())
    logging.info("Telemetry and logging are set up")


__all__ = ["traceFunction", "setupLogging"]