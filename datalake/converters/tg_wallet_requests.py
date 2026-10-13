from typing import List
from topics import TOPIC_TG_WALLET_REQUESTS
from converters.converter import Converter


"""
Telegram native wallet signed requests from tg_wallet_request table, exported as is.
"""
class TgWalletRequestsConverter(Converter):
    def __init__(self):
        super().__init__("schemas/tg_wallet_requests.avsc", ignored_fields=["created", "updated"])

    def timestamp(self, obj):
        return obj['event_time']

    def topics(self) -> List[str]:
        return [TOPIC_TG_WALLET_REQUESTS]
