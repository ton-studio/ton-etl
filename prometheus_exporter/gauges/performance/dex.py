from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class DexPerformanceGauge(PerformanceGauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str],
        platforms: Iterable[str],
        interval: int,
        update_interval: int,
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

    def _calc_metrics(self):
        if not self._data["dex_swap_parsed"]:
            logger.warning("No DEX swap data available for calculating metrics")
            return None

        data_list = [
            self._data["traces"][value["trace_id"]]["delay"]
            for value in self._data["dex_swap_parsed"].values()
            if self._data["traces"].get(value["trace_id"])
        ]

        if not data_list:
            logger.warning("No matching trace data found for DEX swaps")
            return None

        return self._metrics_from_delay(data_list)
