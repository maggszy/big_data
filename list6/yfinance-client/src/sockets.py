import socket
from abc import ABCMeta, abstractmethod


class SocketUser(metaclass=ABCMeta):
    def __init__(self, socket_config: dict) -> None:
        self.host = socket_config.get("host")
        self.port = socket_config.get("port")


class StreamingSender(metaclass=ABCMeta):
    @abstractmethod
    def send_data(self, data, *args, **kwargs):
        raise NotImplementedError


class SocketStreamingServer(SocketUser, StreamingSender):
    def __init__(self, socket_config: dict) -> None:
        super().__init__(socket_config)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(("", self.port))
        self.client_socket.listen(1)

    def send_data(self, data):
        conn, addr = self.client_socket.accept()
        try:
            conn.send(data)
        except ConnectionResetError as e:
            print(f"Error: {e}", flush=True)
        conn.close()

    def disconnect(self):
        self.client_socket.close()


class SocketStreamingClient(SocketUser, StreamingSender):
    def send_data(self, data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            message = s.recv(4)
            if message == b"SIZE":
                data_size = len(data)
                s.sendall(data_size.to_bytes(4, 'big'))
                s.sendall(data)