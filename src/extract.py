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
        self.lista_resultados_tmdb= list()

    def read_data(self)-> pd.DataFrame | None:
        """Method that forces subclass to implement it and is intended for overriding"""
        
        print(f"Tentando ler arquivo {self.file_path} ...")
        try:
            df = self._extract_data()
            print("File read successfully!")
            return df
        except Exception as error:
            print(f"Error while reading {error}")
            raise error

    @abstractmethod
    def _extract_data(self) ->pd.DataFrame:
        """This is an abstract method intended to be overriden by subclass """
        pass

class ExtractorCSV(BaseExtractor):
    """Extractor for csv files """
    
    @override
    def _extract_data(self):
        """Logic to read csv file"""
        return pd.read_csv(self.file_path,sep="|")
       

class ExtractorJson(BaseExtractor):
    """Extractor for read json files"""

    @override
    def _extract_data(self):
        """Logic to read json file"""
        return pd.read_json(self.file_path)

class ExtractorTMDB():
    """Extractor for TMDB"""
    
    def __init__(self, api_key:str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.semaphore = asyncio.Semaphore(40)
        self.lista_resultados_tmdb = list()
        self.limit = httpx.Limits(max_connections=50,max_keepalive_connections=20)

    async def _busca_dados_completo(self,client:httpx.AsyncClient,id):
        """Retorna em formato JSON o id, data de lançamento, orçamento e receita"""
        full_api= f"{self.base_url}/movie/{id}?api_key={self.api_key}"
        async with self.semaphore:
            try:
                resp = await client.get(full_api,timeout=10.0)
                if resp.status_code == 200:
                    dados = resp.json()
                    return {"imdb_id":dados.get("imdb_id"), "data_lancamento":dados.get("release_date"),
                            "orcamento":dados.get("budget"),"receita":dados.get("revenue")}
                return None
            except Exception as error:
                print(f"Não foi possivel fazer os requests \n Erro:{error}")
                raise error
            
    async def _run_async_requests(self,lista_ids:list) -> pd.DataFrame:
        async with httpx.AsyncClient(limits=self.limit,timeout=None) as client:
            tasks = [self._busca_dados_completo(client,id) for id in lista_ids]

            with tqdm(total=len(tasks),desc=f"Extraindo Dados ({len(lista_ids)}) ",unit="filme") as pbar:
                for coro in asyncio.as_completed(tasks):
                    try:
                        resp = await coro
                        if resp:
                            self.lista_resultados_tmdb.append(resp)
                    except Exception as error:
                        print(error)
                        pass 
                    finally:
                        pbar.update(1)
            return self.lista_resultados_tmdb
    def extract_dados_em_lote(self,lista_ids:list) -> list:
        """Método público que o orquestrador irá chamar"""
        print(f"Iniciando extração de {len(lista_ids)} datas na API do TMDB...")
    
        resultados = asyncio.run(self._run_async_requests(lista_ids))
        return resultados  