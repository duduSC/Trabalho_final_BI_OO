CREATE SCHEMA IF NOT EXISTS refined;

-- 1. Tabela de Dimensão: Filme
drop table if exists refined.dim_filme cascade;
CREATE TABLE refined.dim_filme (
    sk_filme SERIAL PRIMARY KEY, -- Chave surrogate (gerada automaticamente)
    imdb_id VARCHAR(20) NOT NULL UNIQUE, -- ID original (ex: tt0000009)
    titulo_principal VARCHAR(255) NOT NULL,
    titulo_original VARCHAR(255)
);

-- 2. Tabela de Dimensão: Gênero
drop table if exists refined.dim_genero cascade;
CREATE TABLE refined.dim_genero (
    sk_genero SERIAL PRIMARY KEY,
    nome VARCHAR(100) UNIQUE
);

-- 3. Tabela de Dimensão: Data (Simplificada para o seu caso)
drop table if exists refined.dim_data cascade;
CREATE TABLE refined.dim_data (
    sk_data INTEGER PRIMARY KEY, -- Formato YYYYMMDD
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
	semestre INTEGER NOT NULL,
	dia_da_semana INTEGER NOT NULL
);

-- 4. Tabela de Fatos: Filme
-- Esta tabela guarda as métricas e chaves para as dimensões
drop table if exists refined.fato_filme cascade;
CREATE TABLE refined.fato_filme (
    sk_filme INTEGER NOT NULL,
    sk_data INTEGER,
    sk_genero_principal INTEGER,
    sk_genero_secundario INTEGER,
    numero_votos INTEGER DEFAULT 0,
    nota_media FLOAT DEFAULT 0,
    orcamento NUMERIC(100,2) DEFAULT 0.0,
    receita NUMERIC(100,2) DEFAULT 0.0,
    tempo_minutos INTEGER DEFAULT 0,
    CONSTRAINT fk_fato_filme FOREIGN KEY (sk_filme) REFERENCES refined.dim_filme (sk_filme),
    CONSTRAINT fk_fato_data FOREIGN KEY (sk_data) REFERENCES refined.dim_data (sk_data),
    CONSTRAINT fk_fato_genero_principal FOREIGN KEY (sk_genero_principal) REFERENCES refined.dim_genero,
    CONSTRAINT fk_fato_genero_secundario FOREIGN KEY (sk_genero_secundario) REFERENCES refined.dim_genero

);