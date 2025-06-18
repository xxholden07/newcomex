-- Script SQL para criação das tabelas conforme o dicionário de dados do CNPJ

-- Tabela EMPRESAS
CREATE TABLE Empresas (
    cnpj_basico CHAR(8) PRIMARY KEY,
    razao_social VARCHAR(255),
    natureza_juridica CHAR(4),
    qualificacao_responsavel CHAR(2),
    capital_social DECIMAL(18,2),
    porte_empresa CHAR(2),
    ente_federativo_responsavel VARCHAR(100)
);

-- Tabela ESTABELECIMENTOS
CREATE TABLE Estabelecimentos (
    cnpj_basico CHAR(8),
    cnpj_ordem CHAR(4),
    cnpj_dv CHAR(2),
    identificador_matriz_filial CHAR(1),
    nome_fantasia VARCHAR(255),
    situacao_cadastral CHAR(2),
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral CHAR(2),
    nome_cidade_exterior VARCHAR(100),
    pais CHAR(3),
    data_inicio_atividade DATE,
    cnae_fiscal_principal CHAR(7),
    cnae_fiscal_secundaria VARCHAR(255),
    tipo_logradouro VARCHAR(50),
    logradouro VARCHAR(255),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cep CHAR(8),
    uf CHAR(2),
    municipio CHAR(7),
    ddd1 CHAR(3),
    telefone1 VARCHAR(20),
    ddd2 CHAR(3),
    telefone2 VARCHAR(20),
    ddd_fax CHAR(3),
    fax VARCHAR(20),
    email VARCHAR(255),
    situacao_especial VARCHAR(100),
    data_situacao_especial DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

-- Tabela SIMPLES
CREATE TABLE Simples (
    cnpj_basico CHAR(8) PRIMARY KEY,
    opcao_simples CHAR(1),
    data_opcao_simples DATE,
    data_exclusao_simples DATE,
    opcao_mei CHAR(1),
    data_opcao_mei DATE,
    data_exclusao_mei DATE
);

-- Tabela SOCIOS
CREATE TABLE Socios (
    cnpj_basico CHAR(8),
    identificador_socio CHAR(1),
    nome_socio VARCHAR(255),
    cnpj_cpf_socio VARCHAR(20),
    qualificacao_socio CHAR(2),
    data_entrada_sociedade DATE,
    pais CHAR(3),
    cpf_representante_legal VARCHAR(11),
    nome_representante_legal VARCHAR(255),
    qualificacao_representante_legal CHAR(2),
    faixa_etaria CHAR(1)
);

-- Tabela PAISES
CREATE TABLE Paises (
    codigo CHAR(3) PRIMARY KEY,
    descricao VARCHAR(100)
);

-- Tabela MUNICIPIOS
CREATE TABLE Municipios (
    codigo CHAR(7) PRIMARY KEY,
    descricao VARCHAR(100)
);

-- Tabela QUALIFICACOES
CREATE TABLE Qualificacoes (
    codigo CHAR(2) PRIMARY KEY,
    descricao VARCHAR(100)
);

-- Tabela NATUREZAS
CREATE TABLE Naturezas (
    codigo CHAR(4) PRIMARY KEY,
    descricao VARCHAR(100)
);

-- Tabela CNAES
CREATE TABLE Cnaes (
    codigo CHAR(7) PRIMARY KEY,
    descricao VARCHAR(255)
);

-- Tabela MOTIVOS
CREATE TABLE Motivos (
    codigo CHAR(2) PRIMARY KEY,
    descricao VARCHAR(100)
);
