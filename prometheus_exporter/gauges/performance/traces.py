from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class TracesPerformanceGauge(PerformanceGauge):
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
        self._trace_states = ["complete", "pending"]

    def _calc_metrics(self):
        if not self._data["traces"]:
            logger.warning("No trace data available for calculating metrics")
            return None

        finished_ops_per_second = (
            len([value for value in self._data["traces"].values() if value["state"] == "complete"]) / self._interval
        )
        pending_operations = len([value for value in self._data["traces"].values() if value["state"] == "pending"])

        return [
            {"labels": ["finished_ops_per_second"], "value": finished_ops_per_second},
            {"labels": ["pending_operations"], "value": pending_operations},
        ]
