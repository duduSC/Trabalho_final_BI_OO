import pandas as pd
from sqlalchemy import create_engine , text
import os
from dotenv import load_dotenv
load_dotenv()
class Loader:
    """Classe responsável por fazer o carregamento dos dados para o local de destino"""

    def __init__(self):
        self.DB_SENHA= os.getenv("DB_SENHA")
        self.engine = create_engine(f'postgresql://eduar:{self.DB_SENHA}@localhost:5432/DW_Filmes_OO')

    def save_data(self,df:pd.DataFrame,schema:str,tabela:str):
        """Guarda o DataFrame transformado no DB na camada de destino."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

                if schema == "refined":
                    conn.execute(text(f"TRUNCATE TABLE {schema}.{tabela} CASCADE"))
                    mode= "append"
                else: 
                    mode = "replace"
                conn.commit()

            print(f"Tentando salvar {tabela} no Banco de Dados no schema: {schema}")
            df.to_sql(index=False,
                      con=self.engine,
                      schema=schema,
                      if_exists=mode,
                      name=tabela)  
        except Exception as error:
            print(f"Erro ao tentar salvar arquivo na camada {schema}! ")
            raise error