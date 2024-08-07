
from abc import ABC, abstractmethod

class Base(ABC):
    
    def __init__(self):
        pass

    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def transform(self, df):
        pass
    
    @abstractmethod
    def load(self,df):
        pass
