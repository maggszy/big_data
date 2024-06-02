import json

from src import (
    Producer,
    RDDHandler,
    SocketStreamingServer,
    SocketStreamingClient,
    SparkSerializer,
    Streamer,
    YahooFinanceFetcher,
    YahooFinancePipeline,
)


def get_config() -> dict:
    with open("config.json", "r") as f:
        config = json.load(f)
    return config


def _setup_producer(config: dict) -> None:
    streaming_server_producer = SocketStreamingServer(
        socket_config=config["socket"]["producer"]
    )
    data_fetcher = YahooFinanceFetcher(ticker=config["data"]["ticker"])
    producer = Producer(
        streaming_server=streaming_server_producer,
        data_fetcher=data_fetcher,
        interval=config["streaming"]["interval"],
    )
    producer.start()


def _setup_processing(config: dict) -> None:
    pipeline = YahooFinancePipeline(
        target_column=config["data"]["column"],
        smoothing_win_size=config["visualization"]["smoothingPeriod"],
    )
    serializer = SparkSerializer()
    streaming_client_dashboard = SocketStreamingClient(
        socket_config=config["socket"]["dashboard"]
    )
    rdd_handler = RDDHandler(
        pipeline=pipeline,
        serializer=serializer,
        streaming_client=streaming_client_dashboard,
    )
    streamer = Streamer(
        socket_config=config["socket"]["producer"],
        rdd_handler=rdd_handler,
        batch_interval=config["streaming"]["interval"],
    )
    streamer.start()


def run_app():
    config = get_config()
    _setup_producer(config=config)
    _setup_processing(config=config)


if __name__ == "__main__":
    run_app()
