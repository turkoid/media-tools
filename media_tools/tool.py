from abc import ABC, abstractmethod


class Tool(ABC):
    @abstractmethod
    def run(self):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def create_parser(subparsers):
        raise NotImplementedError
