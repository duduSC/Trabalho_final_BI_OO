import pandas as pd

class Loader:
    """Classe responsável por fazer o carregamento dos dados para o local de destino"""

    def __init__(self: str,schema:str):
        self.schema = schema

    def save_data(self,df:pd.DataFrame):
        """Guarda o DataFrame transformado no DB na camada de destino."""
        try:
            print(f"Tentando salvar {df} no local {self.output_path}")
            df.to_sql(index=False,schema=self.schema,if_exists="fail")
        except Exception as error:
            print(f"Erro ao tentar salvar arquivo na camada {self.schema}!")
            return error