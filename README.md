# 📊 Refatoração ETL - Trabalho Final de BI (POO)

Este repositório contém a refatoração do Trabalho Final da disciplina de Business Intelligence (BI). O objetivo principal desta nova versão é reescrever o pipeline de dados original aplicando conceitos avançados de **Orientação a Objetos (POO)** e **Engenharia de Software**.

## 🚀 O que mudou?

A versão inicial do projeto cumpria o objetivo de limpar e modelar os dados, mas carecia de escalabilidade. Nesta refatoração, o código foi completamente reestruturado para seguir os princípios **SOLID** (com foco no Princípio da Responsabilidade Única e Princípio Aberto/Fechado) e padrões de projeto como **Template Method**.

### 🛠️ Arquitetura e Pipeline (ETL)

O projeto agora está dividido em módulos independentes:

* **Extract (`src/extract.py`):** Implementação de uma classe abstrata `BaseExtractor` que padroniza o tratamento de erros. Criada a implementação específica `ExtractorCSV` para leitura de dados brutos (focados no dataset do IMDb). A arquitetura permite fácil expansão para ler JSON ou bancos de dados no futuro.
* **Transform (`src/transform.py`):** Lógica isolada em uma classe *stateless* (sem estado). Realiza a limpeza de nulos (tratamento de valores `\N`), remoção de duplicatas e aplica as regras de negócio.
* **Modelagem Dimensional:** O pipeline de transformação converte o *tabelão* bruto em um modelo *Star Schema* otimizado para BI, gerando as tabelas:
    * `dim_filme`
    * `dim_genero`
    * Tabela Ponte (Bridge) associando filmes e gêneros.
* **Quality Assurance (`tests/`):** Implementação de testes automatizados unitários utilizando o framework `pytest` e fixtures como `tmp_path` para garantir a integridade da extração de dados sem poluir o ambiente.

## 💻 Tecnologias Utilizadas

* Python 3
* Pandas (Processamento de Dados)
* Pytest (Testes Unitários)

## 👨‍🎓 Autor

**Eduardo dos Santos de Camargo** Estudante