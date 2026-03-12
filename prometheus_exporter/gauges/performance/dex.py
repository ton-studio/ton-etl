from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class DexPerformanceGauge(PerformanceGauge):
    """Gauge for DEX swap latency metrics (average, p50, p75, p95), filtered by platform."""

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        platforms: Iterable[str] = (),
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
        self._tables = ["blocks", "traces", "dex_swap_parsed"]
        self._platforms = platforms

    def _calc_metrics(self) -> list | None:
        if not self._data["dex_swap_parsed"]:
            logger.debug("No DEX swap data available for calculating metrics")
            return None

        data_list = [
            self._data["traces"][value["trace_id"]]["delay"]
            for value in self._data["dex_swap_parsed"].values()
            if self._data["traces"].get(value["trace_id"])
        ]

        if not data_list:
            logger.debug("No matching trace data found for DEX swaps")
            return None

        return self._metrics_from_delay(data_list)
