from prometheus_client import CollectorRegistry

from gauges.performance.performance import PerformanceGauge


def make_gauge(name: str = "test_gauge") -> PerformanceGauge:
    class ConcreteGauge(PerformanceGauge):
        def _calc_metrics(self) -> list | None:
            return None

    return ConcreteGauge(name, "test", ["col"], registry=CollectorRegistry())


MASTERCHAIN_BLOCK = {
    "__table": "blocks",
    "workchain": -1,
    "shard": -9223372036854775808,
    "seqno": 1,
    "gen_utime": 1000,
    "tx_count": 5,
}


class TestPercentile:
    def test_empty_returns_none(self):
        g = make_gauge()
        assert g._percentile([], 0.5) is None

    def test_single_element(self):
        g = make_gauge()
        assert g._percentile([42], 0.5) == 42

    def test_p50(self):
        g = make_gauge()
        assert g._percentile([1, 2, 3, 4, 5], 0.5) == 3

    def test_p95(self):
        g = make_gauge()
        assert g._percentile(list(range(1, 101)), 0.95) == 95


class TestMetricsFromDelay:
    def test_returns_five_entries(self):
        g = make_gauge()
        result = g._metrics_from_delay([10, 20, 30])
        labels = [m["labels"][0] for m in result]
        assert labels == ["average", "p50", "p75", "p95", "tx_count"]

    def test_average(self):
        g = make_gauge()
        result = g._metrics_from_delay([10, 20, 30])
        avg = next(m["value"] for m in result if m["labels"] == ["average"])
        assert avg == 20

    def test_tx_count(self):
        g = make_gauge()
        result = g._metrics_from_delay([5, 10, 15, 20])
        count = next(m["value"] for m in result if m["labels"] == ["tx_count"])
        assert count == 4


class TestHandleBlocks:
    def test_masterchain_updates_timestamp(self):
        g = make_gauge()
        g._handle_blocks(MASTERCHAIN_BLOCK)
        assert g._last_timestamp == 1000

    def test_timestamp_only_increases(self):
        g = make_gauge()
        g._handle_blocks({**MASTERCHAIN_BLOCK, "gen_utime": 1000})
        g._handle_blocks({**MASTERCHAIN_BLOCK, "gen_utime": 500})
        assert g._last_timestamp == 1000

    def test_non_masterchain_ignored(self):
        g = make_gauge()
        g._handle_blocks({**MASTERCHAIN_BLOCK, "workchain": 0})
        assert g._last_timestamp == 0


class TestHandleTraces:
    def test_complete_trace_stored(self):
        g = make_gauge()
        g._handle_traces({
            "__table": "traces",
            "trace_id": "abc",
            "start_utime": 100,
            "end_utime": 110,
            "state": "complete",
        })
        assert "abc" in g._data["traces"]
        assert g._data["traces"]["abc"]["delay"] == 10

    def test_non_complete_state_filtered_by_default(self):
        g = make_gauge()
        g._handle_traces({
            "__table": "traces",
            "trace_id": "abc",
            "start_utime": 100,
            "end_utime": 110,
            "state": "pending",
        })
        assert "abc" not in g._data["traces"]

    def test_missing_fields_ignored(self):
        g = make_gauge()
        g._handle_traces({"__table": "traces", "trace_id": "abc"})
        assert "abc" not in g._data["traces"]

    def test_node_filter(self):
        g = make_gauge()
        g._trace_nodes = [3]
        g._handle_traces({
            "__table": "traces",
            "trace_id": "abc",
            "start_utime": 100,
            "end_utime": 110,
            "state": "complete",
            "nodes_": 5,
        })
        assert "abc" not in g._data["traces"]


class TestHandleJettonTransfers:
    def test_non_aborted_stored(self):
        g = make_gauge()
        g._handle_jetton_transfers({
            "tx_hash": "hash1",
            "tx_now": 999,
            "trace_id": "t1",
            "tx_aborted": False,
        })
        assert "hash1" in g._data["jetton_transfers"]

    def test_aborted_ignored(self):
        g = make_gauge()
        g._handle_jetton_transfers({
            "tx_hash": "hash1",
            "tx_now": 999,
            "trace_id": "t1",
            "tx_aborted": True,
        })
        assert "hash1" not in g._data["jetton_transfers"]

    def test_missing_fields_ignored(self):
        g = make_gauge()
        g._handle_jetton_transfers({"tx_hash": "hash1"})
        assert "hash1" not in g._data["jetton_transfers"]


class TestHandleDexSwapParsed:
    def test_matching_platform_stored(self):
        g = make_gauge()
        g._platforms = ["dedust"]
        g._handle_dex_swap_parsed({
            "tx_hash": "h1",
            "swap_utime": 500,
            "trace_id": "t1",
            "platform": "dedust",
        })
        assert "h1" in g._data["dex_swap_parsed"]

    def test_wrong_platform_ignored(self):
        g = make_gauge()
        g._platforms = ["dedust"]
        g._handle_dex_swap_parsed({
            "tx_hash": "h1",
            "swap_utime": 500,
            "trace_id": "t1",
            "platform": "ston.fi",
        })
        assert "h1" not in g._data["dex_swap_parsed"]


class TestCleanup:
    def test_stale_records_removed(self):
        g = make_gauge()
        g._last_timestamp = 1000
        g._interval = 100
        g._data["traces"] = {
            "old": {"timestamp": 850, "delay": 5, "state": "complete"},
            "fresh": {"timestamp": 950, "delay": 5, "state": "complete"},
        }
        g._cleanup()
        assert "old" not in g._data["traces"]
        assert "fresh" in g._data["traces"]
