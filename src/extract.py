from typing import override

import pandas as pd;
from abc import ABC, abstractmethod

import asyncio
import httpx
from tqdm import tqdm
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import numpy as np


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

class ExtractorTMDB(BaseExtractor):
    """Extractor for TMDB"""
    
    def __init__(self, api_key:str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.lista_ids = dict()
        self.semaphore = asyncio.Semaphore(40)

    async def _busca_data_completa(self,client:httpx.AsyncClient,id_imdb):
        """Will send async requests to API"""
        
        async with self.semaphore:
            full_api = f"{self.base_url}/find/{id_imdb}?api_key={self.api_key}&external_source=imdb_i"
            try:
                resp = await client.get(full_api,timeout=10.0)
                if resp.status_code == 200:
                    dados = resp.json()
                    results = dados.get("movie_results")
                    data= results[0].get("release_date")
                    self.lista_ids["id_imdb"]= data
                    return {"data":data}
                return None
            except Exception as error:
                print(f"Erro ao buscar filme pelo {id_imdb}\nErro: {error}")
                return None
                
    @override
    def _extract_data(self):
        """That method will send requests to api"""