from abc import ABC, ABCMeta, abstractmethod

from datetime import datetime


class SortingISA(ABC, metaclass=ABCMeta):

    @abstractmethod
    def vector_size(self):
        pass

    @abstractmethod
    def vector_size(self):
        pass

    @abstractmethod
    def vector_type(self):
        pass

    @classmethod
    @abstractmethod
    def supported_types(cls):
        pass

    @abstractmethod
    def generate_prologue(self, f):
        pass

    @abstractmethod
    def generate_epilogue(self, f):
        pass

    @abstractmethod
    def generate_entry_points(self, f):
        pass

    @abstractmethod
    def generate_master_entry_point(self, f):
        pass
