import pandas as pd
import numpy as np
class Transform:
    """Class that is responsable to transform the Dataframe que é responsável pela transformação do DataFrame"""


    @staticmethod
    def process_data(df:pd.DataFrame) -> pd.DataFrame:
        """Apply bussines rules"""
        try:
            df_limpo = df.copy()
            df_limpo =(
                df_limpo.replace([r"\N",None,""],pd.NA)
                .drop_duplicates(['id'])
                .dropna()
                .drop(columns=["personagem","nomeArtista","anoNascimento","anoFalecimento","profissao","titulosMaisConhecidos","generoArtista"])
                .rename(columns={
                    "id":"imdb_id",
                    "tituloPincipal":"titulo_principal",
                    "tituloOriginal":"titulo_original",
                    "notaMedia":"nota_media",
                    "numeroVotos":"numero_votos",
                    "tempoMinutos":"tempo_minutos"})
                )
            
            df_limpo["genero"] = df_limpo["genero"].str.split(",")
            df_limpo["genero_principal"] = df_limpo["genero"].str.get(0)
            df_limpo["genero_secundario"] = df_limpo["genero"].str.get(1)
            df_limpo = df_limpo[df_limpo["numero_votos"] > 100]
            return df_limpo
        
        except Exception as error:
            print(f"Error : {error}")
            raise error
    
    @staticmethod
    def create_dim_filme(df_limpo:pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_filme"""
        try:

            dim_filme = df_limpo.copy()
            dim_filme = dim_filme[["imdb_id","titulo_principal","titulo_original"]]
            dim_filme["imdb_id"]= dim_filme["imdb_id"].astype(str)
            dim_filme["sk_filme"] = range(len(dim_filme["imdb_id"]))
            return dim_filme
        except Exception as error:
            raise error
    @staticmethod
    def create_dim_genero( df_limpo:pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_genero"""
        
        dim_genero= df_limpo[["genero"]]
        dim_genero = (
            dim_genero.explode("genero")
            .rename(columns={"genero":"nome"})
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_genero = pd.DataFrame(dim_genero)
        dim_genero["sk_genero"]= range(len(dim_genero))
        dim_genero = dim_genero[["sk_genero", "nome"]]
        dim_genero.loc[len(dim_genero)] = [999,"sem gênero"]
        dim_genero =dim_genero.drop_duplicates()
        return dim_genero
    
    @staticmethod
    def create_bridge_filme_genero(df_limpo:pd.DataFrame,dim_genero:pd.DataFrame,dim_filme: pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_data"""
        
        df_explode = df_limpo.copy()
        df_explode = df_explode[["imdb_id","genero"]]
        df_explode = (
            df_explode.explode("genero")
            .rename(columns={"id":"imdb_id"})
        )
        
        df_bridge = df_explode.merge(dim_genero,left_on="genero",right_on="nome",how="inner")
        df_bridge = df_bridge.merge(dim_filme,on="imdb_id",how="inner")
        df_bridge = df_bridge[["sk_filme","sk_genero"]]
        return df_bridge
    
    @staticmethod
    def create_dim_data(df: pd.DataFrame):
        """Create dim_data"""
        df_limpo = df.copy()
        df_limpo["data_lancamento"] = pd.to_datetime(df_limpo["data_lancamento"],errors="coerce")

        df_limpo = df_limpo.dropna(subset=["data_lancamento"])
        dim_data = df_limpo.copy()
        dim_data = df_limpo.assign(
            ano = (dim_data["data_lancamento"].dt.year).astype(int),
            mes = (dim_data["data_lancamento"].dt.month).astype(int),
            dia= (dim_data["data_lancamento"].dt.month).astype(int),
            semestre = np.where(dim_data["data_lancamento"].dt.quarter>2,1,2),
            dia_da_semana= (dim_data["data_lancamento"].dt.day_of_week).astype(int)
        )
        dim_data["sk_data"] = dim_data["data_lancamento"].astype(str).str.replace("-","")
        dim_data = dim_data.drop(columns=["data_lancamento"])
        dim_data= dim_data[["sk_data","ano","mes","dia","semestre","dia_da_semana"]]
        dim_data = dim_data.drop_duplicates()
        return dim_data
    
    @staticmethod
    def create_fato_filme(df_limpo: pd.DataFrame,dim_filme:pd.DataFrame,dim_genero:pd.DataFrame,dim_data:pd.DataFrame):
       
        fato_filme = df_limpo.copy()
        fato_filme = pd.merge(fato_filme,dim_filme[["sk_filme","imdb_id"]],on="imdb_id",how="inner")

        fato_filme["data_lancamento"] = fato_filme["data_lancamento"].astype(str).str.replace("-","")

        fato_filme = pd.merge(fato_filme,dim_data,left_on="data_lancamento",right_on="sk_data",how="inner")
        dict_generos = dict(zip(dim_genero["nome"],dim_genero["sk_genero"]))
        dict_generos[pd.NA] = 999
        
        fato_filme["sk_genero_principal"] = fato_filme["genero_principal"].map(dict_generos)
        fato_filme["sk_genero_secundario"] = fato_filme["genero_secundario"].map(dict_generos)


        fato_filme["sk_genero_principal"] = fato_filme["sk_genero_principal"].astype("Int64")
        fato_filme["sk_genero_secundario"] = fato_filme["sk_genero_secundario"].astype("Int64")
        

        fato_filme = fato_filme[["sk_filme","sk_data","sk_genero_principal","sk_genero_secundario",
                                 "numero_votos","nota_media","orcamento","receita","tempo_minutos"]]
        return fato_filme