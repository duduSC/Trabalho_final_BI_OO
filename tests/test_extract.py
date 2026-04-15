import pytest
import pandas as pd
from src.extract import ExtractorCSV, ExtractorTMDB
import os
from dotenv import load_dotenv
import httpx

def test_leitura_de_csv_com_sucesso(tmp_path):
    # Arrange
    arquivo_falso = tmp_path/ "dados_teste.csv"
    arquivo_falso.write_text("id,produto\n1,telefone\n2,copo")

    extractor = ExtractorCSV(str(arquivo_falso))

    # Act
    df_resultado = extractor.read_data()

    #Assert
    assert df_resultado is not None

    assert len(df_resultado) ==2

    assert df_resultado["produto"][0] == "telefone"

def test_leitura_de_csv_com_erro():
    #Arrange
    extractor= ExtractorCSV("caminho_errado/dados.csv")

    #Act
    df_resultado = extractor.read_data()

    #Assert 
    assert df_resultado is None
    
async def test_extract_tmdb_com_sucesso():

    #Arrange
    load_dotenv()
    API_KEY=os.getenv("API_KEY")
    extractor = ExtractorTMDB(API_KEY)
    imdb_id = "tt0000009"
    mock_json = {"imdb_id":imdb_id, "data_lancamento":"1894-10-08",
                            "orcamento":0,"receita":0}

    #Act 
    async with httpx.AsyncClient() as client:
        json_resultado = await extractor.busca_dados_completo(client,imdb_id)
    
    #Assert
    assert json_resultado is not None
    assert json_resultado == mock_json


async def test_extract_tmdb_com_erro():

    #Arrange
    load_dotenv()
    API_KEY=os.getenv("API_KEY")
    extractor = ExtractorTMDB(API_KEY)
    imdb_id = "tt000000"
    #Act 
    async with httpx.AsyncClient() as client:
        json_resultado = await extractor.busca_dados_completo(client,imdb_id)
    
    #Assert
    assert json_resultado is None
