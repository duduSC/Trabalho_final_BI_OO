import pytest
import pandas as pd
from src.extract import ExtractorCSV

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

def test_leitura_de_csv_com_erro(tmp_path):
    #Arrange
    extractor= ExtractorCSV("caminho_errado/dados.csv")

    #Act
    df_resultado = extractor.read_data()

    #Assert 
    assert df_resultado is None
    

    