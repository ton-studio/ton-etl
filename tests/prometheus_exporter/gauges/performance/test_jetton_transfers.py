from prometheus_client import CollectorRegistry

from gauges.performance.jetton_transfers import JettonTransfersPerformanceGauge


def make_jetton() -> JettonTransfersPerformanceGauge:
    return JettonTransfersPerformanceGauge("test_jetton", "test", ["col"], registry=CollectorRegistry())


class TestJettonTransfersCalcMetrics:
    def test_no_transfers_returns_none(self):
        g = make_jetton()
        assert g._calc_metrics() is None

    def test_no_matching_traces_returns_none(self):
        g = make_jetton()
        g._data["jetton_transfers"] = {"h1": {"timestamp": 100, "trace_id": "missing"}}
        assert g._calc_metrics() is None

    def test_returns_delay_metrics(self):
        g = make_jetton()
        g._data["traces"] = {"t1": {"timestamp": 110, "delay": 20, "state": "complete"}}
        g._data["jetton_transfers"] = {"h1": {"timestamp": 110, "trace_id": "t1"}}
        result = g._calc_metrics()
        assert result is not None
        avg = next(m["value"] for m in result if m["labels"] == ["average"])
        assert avg == 20

    def test_filters_by_trace_nodes(self):
        g = make_jetton()
        assert g._trace_nodes == [3, 4, 5]
