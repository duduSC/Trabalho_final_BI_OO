from src.extract import ExtractorCSV,ExtractorTMDB
from src.transform import Transform
from src.load import Loader
import os
from dotenv import load_dotenv
import httpx


load_dotenv()
class PipelineETL():
    """Orquestra as 3 etapas do pipeline"""
    def __init__(self,input_path: str):
        self.API_KEY = os.getenv("API_KEY")
        self.extractor_csv= ExtractorCSV(input_path)
        self.extractor_tmdb = ExtractorTMDB(self.API_KEY)
        self.transform = Transform()
        self.load = Loader()
        self.limit = httpx.Limits(max_connections=50,max_keepalive_connections=20)


    async def run(self):
        print("=== Iniciando o Processo ETL ===")

        df_lido = self.extractor_csv.read_data()
            
        df_limpo = self.transform.process_data(df_lido)

        dim_filme = self.transform.create_dim_filme(df_limpo)

        dim_genero = self.transform.create_dim_genero()

        bridge_filme_genero = self.transform.create_bridge_filme_genero(df_limpo,dim_genero)
