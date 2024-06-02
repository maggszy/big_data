from abc import ABCMeta, abstractmethod
import socket
from threading import Thread
from typing import Any


class SocketUser(metaclass=ABCMeta):
    def __init__(self, socket_config: dict) -> None:
        self.host = socket_config.get("host")
        self.port = socket_config.get("port")


class Receiver(metaclass=ABCMeta):
    @abstractmethod
    def receive(self):
        raise NotImplementedError


class SocketReceivingServer(SocketUser, Receiver):
    def __init__(self, socket_config: dict) -> None:
        super().__init__(socket_config)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("", self.port))
        self.server_socket.listen(1)

    @staticmethod
    def _safe_receive(conn: socket.socket, data_size: int) -> bytes:
        size_received = 0
        data = b""
        while True:
            data_portion = conn.recv(data_size - size_received)
            if data_portion:
                data += data_portion
                size_received += len(data_portion)
            else:
                break
        return data

    def receive(self):
        conn, addr = self.server_socket.accept()
        with conn:
            conn.sendall(b"SIZE")

            size_data = conn.recv(4)
            if not size_data:
                print("Failed to receive data size", flush=True)
                return None

            data_size = int.from_bytes(size_data, "big")
            data = self._safe_receive(conn, data_size)

            if len(data) != data_size:
                print("Failed to receive full data")
                return None

        return data

    def disconnect(self):
        self.server_socket.close()


class ActionHandler:

    def __init__(
        self,
        function: callable,
        args: tuple = (),
        kwargs: dict = dict(),
    ) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.data = None

    def feed_data(self, data: Any) -> None:
        self.data = data

    def call(self) -> None:
        self.function(self.data, *self.args, **self.kwargs)


class SocketListener:
    def __init__(self, receiver: Receiver) -> None:
        self._receiver: Receiver = receiver
        self._handlers: set[ActionHandler] = set()

    def add_handler(self, handler: ActionHandler) -> None:
        self._handlers.add(handler)

    def remove_handler(self, handler) -> None:
        self._handlers.remove(handler)

    @property
    def handlers(self) -> set:
        return self._handlers

    def _loop(self) -> None:
        while True:
            data = self._receiver.receive()
            if data is None:
                continue
            for handler in self._handlers:
                handler.feed_data(data)
                handler.call()

    def listen(self) -> None:
        self._thread = Thread(target=self._loop)
        self._thread.start()
