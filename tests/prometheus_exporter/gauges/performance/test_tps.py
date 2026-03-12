import pytest
from prometheus_client import CollectorRegistry

from gauges.performance.tps import TPSPerformanceGauge


def make_tps() -> TPSPerformanceGauge:
    return TPSPerformanceGauge("test_tps", "test", ["col"], registry=CollectorRegistry())


class TestTPSCalcMetrics:
    def test_no_blocks_returns_none(self):
        g = make_tps()
        assert g._calc_metrics() is None

    def test_tps_calculation(self):
        g = make_tps()
        g._interval = 10
        g._data["blocks"] = {
            (0, 0, 1): {"timestamp": 100, "tx_count": 5},
            (0, 0, 2): {"timestamp": 105, "tx_count": 5},
        }
        result = g._calc_metrics()
        assert result is not None
        tps_entry = next(m for m in result if m["labels"] == ["tps"])
        assert tps_entry["value"] == pytest.approx(1.0)
