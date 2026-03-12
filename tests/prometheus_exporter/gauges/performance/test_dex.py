from prometheus_client import CollectorRegistry

from gauges.performance.dex import DexPerformanceGauge


def make_dex(platforms=("dedust",)) -> DexPerformanceGauge:
    return DexPerformanceGauge("test_dex", "test", ["col"], platforms=platforms, registry=CollectorRegistry())


class TestDexCalcMetrics:
    def test_no_swaps_returns_none(self):
        g = make_dex()
        assert g._calc_metrics() is None

    def test_no_matching_traces_returns_none(self):
        g = make_dex()
        g._data["dex_swap_parsed"] = {"h1": {"timestamp": 100, "trace_id": "missing_trace"}}
        assert g._calc_metrics() is None

    def test_returns_delay_metrics(self):
        g = make_dex()
        g._data["traces"] = {"t1": {"timestamp": 110, "delay": 10, "state": "complete"}}
        g._data["dex_swap_parsed"] = {"h1": {"timestamp": 110, "trace_id": "t1"}}
        result = g._calc_metrics()
        assert result is not None
        avg = next(m["value"] for m in result if m["labels"] == ["average"])
        assert avg == 10
