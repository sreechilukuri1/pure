from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class noop_span:
    def __enter__(self): return None
    def __exit__(self, *args): pass


def start_span(name):
    try:
        return tracer.start_as_current_span(name)
    except Exception:
        return noop_span()