import pandas as pd;

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
                .drop(columns=[
                    "personagem","nomeArtista","anoNascimento","anoFalecimento","profissao",
                    "titulosMaisConhecidos","generoArtista"])
                .rename(columns={
                    "id":"id_imdb",
                    "tituloPincipal":"titulo_principal",
                    "tituloOriginal":"titulo_original",
                    "notaMedia":"nota_media",
                    "numeroVotos":"numero_votos",
                    "tempoMinutos":"tempo_minutos"})
                .dropna()

                )

            df_limpo["genero"] = df_limpo["genero"].str.split(",")
            df_limpo = df_limpo[df_limpo["numero_votos"] > 100]
            return df_limpo
        
        except Exception as error:
            print(f"Error : {error}")
            return None
    
    @staticmethod
    def create_dim_filme(df_limpo:pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_filme"""
        dim_filme = df_limpo.copy()
        dim_filme = dim_filme[["id_imdb","titulo_principal","titulo_original"]]
        dim_filme["id_imdb"]= dim_filme["id_imdb"].astype(str)
        return dim_filme
    @staticmethod
    def create_dim_genero( df_limpo:pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_genero"""
        
        dim_genero= df_limpo[["genero"]].copy()
        dim_genero = (
            dim_genero.explode("genero")
            .rename("nome")
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_genero = pd.DataFrame(dim_genero)
        dim_genero["id_genero"]= range(len(dim_genero))
        dim_genero = dim_genero[["id_genero", "nome"]]

        return dim_genero
    
    @staticmethod
    def create_bridge_filme_genero(df_limpo:pd.DataFrame,dim_genero:pd.DataFrame)-> pd.DataFrame:
        """Create DF dim_data"""
        
        df_explode = df_limpo.copy()
        df_explode = df_explode[["id_imdb","genero"]]
        df_explode = (
            df_explode.explode("genero")
            .rename(columns={"id":"id_imdb","genero":"nome"})
        )
        
        df_bridge = df_explode.merge(dim_genero,on="nome",how="inner")
        df_bridge= df_bridge.rename(columns={"id":"id_genero"})
        return df_bridge
    
    @staticmethod
    def create_dim_data(df: pd.DataFrame):
        """Create dim_data"""
        df_limpo = df.copy()
        df_limpo["data"] = pd.to_datetime(df_limpo,errors="coerce")

        df_limpo = df_limpo.dropna(subset=["data"])
        dim_data = df_limpo.copy()
        dim_data = df_limpo.assign(
            ano = (dim_data["data"].dt.year).astype(int),
            mes = (dim_data["data"].dt.month).astype(int),
            dia= (dim_data["data"].dt.month).astype(int),
            semestre = np.where(dim_data["data"].dt.quarter>2,1,2),
            dia_da_semana= (dim_data["data"].dt.day_of_week).astype(int)
        )
        dim_data["sk_data"] = dim_data["data"].astype(str).replace("-","")
        dim_data = dim_data.drop(columns=["data"])
        return dim_data

    def create_fato_filme()