from typing import override

import pandas as pd;
from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    """Base abstract class"""

    def __init__(self,file_path: str):
        self.file_path = file_path

    def read_data(self)-> pd.DataFrame | None:
        """Method that forces subclass to implement it and is intended for overriding"""
        
        print(f"Tentando ler arquivo {self.file_path} ...")
        try:
            df = self._extract_data()
            print("File read successfully!")
            return df
        except Exception as error:
            print(f"Error while reading {error}")

    @abstractmethod
    def _extract_data(self) ->pd.DataFrame:
        """This is an abstract method intended to be overriden by subclass """
        pass

class ExtractorCSV(BaseExtractor):
    """Extractor for csv files """
    
    @override
    def _extract_data(self):
        """Logic to read csv file"""
        return pd.read_csv(self.file_path)
       

class ExtractorJson(BaseExtractor):
    """Extractor for read json files"""

    @override
    def _extract_data(self):
        """Logic to read json file"""
        return pd.read_json(self.file_path)