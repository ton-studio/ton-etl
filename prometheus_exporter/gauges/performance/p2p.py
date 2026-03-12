from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class P2pPerformanceGauge(PerformanceGauge):
    """Gauge for P2P transfer latency metrics (average, p50, p75, p95), tracking 2-node traces."""

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        interval: int = 600,
        update_interval: int = 5,
        **kwargs,
    ):
        super().__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            interval=interval,
            update_interval=update_interval,
            **kwargs,
        )
        self._tables = ["blocks", "traces"]
        self._trace_nodes = [2]

    def _calc_metrics(self) -> list | None:
        if not self._data["traces"]:
            logger.debug("No trace data available for calculating metrics")
            return None

        data_list = [value["delay"] for value in self._data["traces"].values()]

        return self._metrics_from_delay(data_list)
