import json

from src import Dashboard, SocketReceivingServer, SocketListener, ActionHandler


def get_config() -> dict:
    with open("config.json", "r") as f:
        config = json.load(f)
    return config


def run_app():
    config = get_config()
    streaming_receiver = SocketReceivingServer(
        socket_config=config["socket"]["dashboard"]
    )
    socket_listener = SocketListener(streaming_receiver)

    dashboard = Dashboard(stock_name=config["visualization"]["name"])

    refresh_handler = ActionHandler(dashboard.call_refresh)
    socket_listener.add_handler(refresh_handler)
    socket_listener.listen()

    dashboard.run()


if __name__ == "__main__":
    run_app()
