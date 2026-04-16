import sys
import traceback

from src.extract import ExtractorCSV,ExtractorTMDB
from src.transform import Transform
from src.load import Loader
import os
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

class PipelineETL():
    """Orquestra as 3 etapas do pipeline"""
    def __init__(self,input_path: str):
        self.API_KEY = os.getenv("API_KEY")
        self.extractor_csv= ExtractorCSV(input_path)
        self.extractor_tmdb = ExtractorTMDB(self.API_KEY)
        self.transform = Transform()
        self.load = Loader()


    def run(self):
        print("=== Iniciando o Processo ETL ===")

        try:
            # Camada RAW
            df_lido = self.extractor_csv.read_data()    
            self.load.save_data(df_lido,"raw","arquivo_csv")
            # Camada TRUSTED
            df_limpo = self.transform.process_data(df_lido)
            self.load.save_data(df_limpo,"trusted","arquivo_csv")

            # Preparação para Camada REFINED
            dim_filme = self.transform.create_dim_filme(df_limpo)
            dim_genero = self.transform.create_dim_genero(df_limpo)
            
            # Extração da API e camada RAW
            lista_ids = dim_filme["imdb_id"].to_list()
            lista_tmdb = self.extractor_tmdb.extract_dados_em_lote(lista_ids)
            df_tmdb = pd.DataFrame(lista_tmdb)
            self.load.save_data(df_tmdb,"raw","arquivo_json")

            # Camada TRUSTED
            df_limpo= df_limpo.merge(df_tmdb,on="imdb_id",how="inner")
            
            self.load.save_data(df_limpo,"trusted","df_movies")

            # Camada REFINED
            dim_data = self.transform.create_dim_data(df_limpo)
            fato_filme = self.transform.create_fato_filme(df_limpo,dim_filme,dim_genero,dim_data)

            self.load.save_data(dim_filme,"refined","dim_filme")
            self.load.save_data(dim_genero,"refined","dim_genero")
            self.load.save_data(dim_data,"refined","dim_data")
            self.load.save_data(fato_filme,"refined","fato_filme")
            
        except Exception as fatal_error:
            print("\n" + "="*50)
            print("🚨 FALHA CRÍTICA NO PIPELINE 🚨")
            print("="*50)
            print("\nMatando a execução para proteger a integridade dos dados...")
            print(traceback.print_exc())
            # O código 1 avisa ao sistema operacional que o programa falhou
            sys.exit(1) 