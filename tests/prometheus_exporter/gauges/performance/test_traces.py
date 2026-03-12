import pytest
from prometheus_client import CollectorRegistry

from gauges.performance.traces import TracesPerformanceGauge


def make_traces() -> TracesPerformanceGauge:
    return TracesPerformanceGauge("test_traces", "test", ["col"], registry=CollectorRegistry())


class TestTracesCalcMetrics:
    def test_no_traces_returns_none(self):
        g = make_traces()
        assert g._calc_metrics() is None

    def test_counts_finished_ops_per_second(self):
        g = make_traces()
        g._interval = 10
        g._data["traces"] = {
            "t1": {"timestamp": 100, "delay": 5, "state": "complete"},
            "t2": {"timestamp": 101, "delay": 3, "state": "complete"},
            "t3": {"timestamp": 102, "delay": 2, "state": "pending"},
        }
        result = g._calc_metrics()
        assert result is not None
        ops = next(m["value"] for m in result if m["labels"] == ["finished_ops_per_second"])
        assert ops == pytest.approx(0.2)  # 2 complete / interval=10

    def test_counts_pending_operations(self):
        g = make_traces()
        g._interval = 10
        g._data["traces"] = {
            "t1": {"timestamp": 100, "delay": 5, "state": "complete"},
            "t2": {"timestamp": 101, "delay": 3, "state": "pending"},
            "t3": {"timestamp": 102, "delay": 2, "state": "pending"},
        }
        result = g._calc_metrics()
        pending = next(m["value"] for m in result if m["labels"] == ["pending_operations"])
        assert pending == 2

    def test_accepts_both_states(self):
        g = make_traces()
        assert "complete" in g._trace_states
        assert "pending" in g._trace_states
