from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class TPSPerformanceGauge(PerformanceGauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        interval: int = 100,
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
        self._tables = ["blocks"]

    def _handle_blocks(self, obj: dict):
        super()._handle_blocks(obj)

        workchain = obj.get("workchain")
        shard = obj.get("shard")
        seqno = obj.get("seqno")
        gen_utime = obj.get("gen_utime")
        tx_count = obj.get("tx_count")

        if None in (workchain, shard, seqno, gen_utime, tx_count):
            return

        if tx_count > 0:
            self._data["blocks"][(workchain, shard, seqno)] = {
                "timestamp": gen_utime,
                "tx_count": tx_count,
            }

    def _calc_metrics(self):
        if not self._data["blocks"]:
            logger.warning("No block data available for calculating metrics")
            return None

        tps = sum([value["tx_count"] for value in self._data["blocks"].values()]) / self._interval

        return [
            {"labels": ["tps"], "value": tps},
        ]
