from src.transform import Transform
import pandas as pd

def test_process_data_com_sucesso():
    #Arrange
    dados_falsos = {
        "id": ["tt001", "tt001", "tt002", "tt003", "tt004"], # tt001 está duplicado
        "tituloPincipal": ["Filme A", "Filme A", "Filme B", "Filme C", "Filme D"],
        "tituloOriginal": ["Movie A", "Movie A", "Movie B", "Movie C", "Movie D"],
        "notaMedia": [8.0, 8.0, 5.5, 9.0, 7.0],
        "numeroVotos": [150, 150, 50, 2000, 500], # tt002 tem 50 votos (deve ser excluído)
        "genero": ["Action,Drama", "Action,Drama", "Comedy", "Sci-Fi", r"\N"], # tt004 tem \N (deve ser excluído)
        
        # Colunas que devem ser apagadas pelo código
        "personagem": ["Herói", "Herói", "Vilão", "Alien", "Mocinho"], 
        "nomeArtista": ["João", "João", "Maria", "José", "Ana"],
        "anoNascimento": [1980, 1980, 1990, 1975, 1985],
        "anoFalecimento": [None, None, None, None, None],
        "profissao": ["Ator", "Ator", "Atriz", "Ator", "Atriz"],
        "titulosMaisConhecidos": ["t1", "t1", "t2", "t3", "t4"],
        "generoArtista": ["M", "M", "F", "M", "F"]
    }
    df_falso = pd.DataFrame(dados_falsos)
    transformador = Transform()
    #Act
    df_resultado = transformador.process_data(df_falso)


    #Assert
    assert len(df_resultado) == 2
    
    # Verifica se as colunas foram apagadas corretamente (a coluna 'personagem' não pode estar lá)
    assert "personagem" not in df_resultado.columns
    
    # Verifica se o rename funcionou (a antiga 'id' agora tem que se chamar 'id_imdb')
    assert "id_imdb" in df_resultado.columns
    assert "id" not in df_resultado.columns

    # Verifica se o texto "Action,Drama" virou uma lista ["Action", "Drama"]
    # Pegamos o valor da primeira linha que sobrou
    lista_de_generos = df_resultado.iloc[0]["genero"] 
    assert isinstance(lista_de_generos, list)
    assert "Action" in lista_de_generos
    assert "Drama" in lista_de_generos

def test_create_dim_filme():
    dados_limpos = {
        # Colocamos o ID como número inteiro (int) de propósito para testar a conversão
        "id_imdb": [12345, 67890], 
        "titulo_principal": ["O Poderoso Chefão", "Matrix"],
        "titulo_original": ["The Godfather", "The Matrix"],
        # Colunas extras que devem ser descartadas pela sua função
        "nota_media": [9.2, 8.7], 
        "genero": [["Crime", "Drama"], ["Sci-Fi", "Action"]] 
    }

    df_dados= pd.DataFrame(dados_limpos)
    transformador = Transform()

    #Act
    dim_filme = transformador.create_dim_filme(df_dados)

    #Assert
    assert len(dim_filme) == 2

    # Verifica se as 3 colunas vitais estão lá
    assert "id_imdb" in dim_filme.columns
    assert "titulo_principal" in dim_filme.columns
    assert "titulo_original" in dim_filme.columns
    
    # Verifica se as colunas extras realmente sumiram
    assert "nota_media" not in dim_filme.columns
    assert "genero" not in dim_filme.columns
    

    assert isinstance(dim_filme["id_imdb"].iloc[0], str)