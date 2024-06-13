import time
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Thread

import requests
from src.sockets import StreamingSender


class DataFetcher(metaclass=ABCMeta):
    @abstractmethod
    def get_data(self, *args, **kwargs):
        raise NotImplementedError


class YahooFinanceFetcher(DataFetcher):
    base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
    headers = {"user-agent": "Mozilla/5.0"}

    def __init__(self, ticker: str, time_span_h: int, time_granularity: str) -> None:
        self.ticker = ticker
        self.time_span_h = time_span_h
        self.time_granularity = time_granularity

    @property
    def params(self) -> dict:
        time_diff = timedelta(hours=self.time_span_h)
        stop_date = datetime.now()
        start_date = stop_date - time_diff
        return {
            "period1": int(datetime.timestamp(start_date)),
            "period2": int(datetime.timestamp(stop_date)),
            "interval": self.time_granularity,
        }

    def fetch(self) -> None:
        url = f"{self.base_url}{self.ticker}"
        response = requests.get(url=url, headers=self.headers, params=self.params)
        print(response.url)
        return response

    def get_data(self) -> None:
        response = self.fetch()
        return response.content


@dataclass
class Producer:
    streaming_server: StreamingSender
    data_fetcher: DataFetcher
    interval: int = 5

    def _loop(self) -> None:
        while True:
            data = self.data_fetcher.get_data()
            self.streaming_server.send_data(data)
            time.sleep(self.interval)

    def start(self) -> None:
        self._thread = Thread(target=self._loop)
        self._thread.start()
