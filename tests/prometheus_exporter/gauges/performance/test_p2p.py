from prometheus_client import CollectorRegistry

from gauges.performance.p2p import P2pPerformanceGauge


def make_p2p() -> P2pPerformanceGauge:
    return P2pPerformanceGauge("test_p2p", "test", ["col"], registry=CollectorRegistry())


class TestP2pCalcMetrics:
    def test_no_traces_returns_none(self):
        g = make_p2p()
        assert g._calc_metrics() is None

    def test_returns_delay_metrics(self):
        g = make_p2p()
        g._data["traces"] = {
            "t1": {"timestamp": 100, "delay": 5, "state": "complete"},
            "t2": {"timestamp": 101, "delay": 15, "state": "complete"},
        }
        result = g._calc_metrics()
        assert result is not None
        avg = next(m["value"] for m in result if m["labels"] == ["average"])
        assert avg == 10

    def test_filters_by_trace_nodes(self):
        g = make_p2p()
        assert g._trace_nodes == [2]
