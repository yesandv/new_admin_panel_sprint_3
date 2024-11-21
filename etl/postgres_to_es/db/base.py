from abc import ABC, abstractmethod
from datetime import datetime


class BaseConnector(ABC):

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def fetch_data(self, query: str, params: tuple) -> list[dict]:
        pass


class DataExtractor(ABC):

    @abstractmethod
    def get_modified_records(self, last_modified: datetime) -> list[str]:
        pass

    @abstractmethod
    def get_data(self, record_ids: list[str]) -> list[dict]:
        pass
