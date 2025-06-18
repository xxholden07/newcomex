import os
import sqlite3
import glob
import polars as pl

# Caminho do banco e da pasta de arquivos parquet
db_path = 'cnpj.db'
parquet_dir = 'dados_parquet'

# Dicionário de nomes de colunas para cada tabela (conforme layout oficial)
colunas_tabelas = {
    'Empresas': [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ],
    'Estabelecimentos': [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
        'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
        'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
        'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
        'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2',
        'ddd_fax', 'fax', 'email', 'situacao_especial', 'data_situacao_especial'
    ],
    'Simples': [
        'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
    ],
    'Socios': [
        'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'cpf_representante_legal',
        'nome_representante_legal', 'qualificacao_representante_legal', 'faixa_etaria'
    ],
    'Paises': ['codigo', 'descricao'],
    'Municipios': ['codigo', 'descricao'],
    'Qualificacoes': ['codigo', 'descricao'],
    'Naturezas': ['codigo', 'descricao'],
    'Cnaes': ['codigo', 'descricao'],
    'Motivos': ['codigo', 'descricao']
}

# Mapeamento dos arquivos para as tabelas
arquivos_tabelas = {
    'Empresas': sorted(glob.glob(os.path.join(parquet_dir, 'Empresas*_EMPRECSV.parquet'))),
    'Estabelecimentos': sorted(glob.glob(os.path.join(parquet_dir, 'Estabelecimentos*_ESTABELE.parquet'))),
    'Simples': [os.path.join(parquet_dir, 'Simples_D50510.parquet')],
    'Socios': sorted(glob.glob(os.path.join(parquet_dir, 'Socios*_SOCIOCSV.parquet'))),
    'Paises': [os.path.join(parquet_dir, 'Paises_PAISCSV.parquet')],
    'Municipios': [os.path.join(parquet_dir, 'Municipios_MUNICCSV.parquet')],
    'Qualificacoes': [os.path.join(parquet_dir, 'Qualificacoes_QUALSCSV.parquet')],
    'Naturezas': [os.path.join(parquet_dir, 'Naturezas_NATJUCSV.parquet')],
    'Cnaes': [os.path.join(parquet_dir, 'Cnaes_CNAECSV.parquet')],
    'Motivos': [os.path.join(parquet_dir, 'Motivos_MOTICSV.parquet')],
}

def remover_duplicatas(df, tabela):
    if tabela == 'Empresas':
        return df.unique(subset=['cnpj_basico'])
    elif tabela == 'Estabelecimentos':
        return df.unique(subset=['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'])
    elif tabela == 'Simples':
        return df.unique(subset=['cnpj_basico'])
    elif tabela == 'Socios':
        return df.unique(subset=['cnpj_basico', 'identificador_socio', 'nome_socio'])
    elif tabela in ['Paises', 'Municipios', 'Qualificacoes', 'Naturezas', 'Cnaes', 'Motivos']:
        return df.unique(subset=['codigo'])
    else:
        return df

def importar_parquet_para_sqlite_polars(tabela, arquivos, conn):
    for arquivo in arquivos:
        print(f'Importando {arquivo} para {tabela}...')
        df = pl.read_parquet(arquivo)
        df.columns = colunas_tabelas[tabela]
        df = remover_duplicatas(df, tabela)
        df_pd = df.to_pandas()
        df_pd.to_sql(tabela, conn, if_exists='append', index=False)

def criar_tabela_importacao(conn):
    sql = '''CREATE TABLE IF NOT EXISTS Importacao (
        CO_ANO INTEGER,
        CO_MES INTEGER,
        CO_NCM INTEGER,
        CO_UNID INTEGER,
        CO_PAIS INTEGER,
        SG_UF_NCM TEXT,
        CO_VIA INTEGER,
        CO_URF INTEGER,
        QT_ESTAT INTEGER,
        KG_LIQUIDO INTEGER,
        VL_FOB INTEGER,
        VL_FRETE INTEGER,
        VL_SEGURO INTEGER,
        CO_BLOCO REAL,
        NO_BLOCO TEXT,
        NO_BLOCO_ING TEXT,
        NO_BLOCO_ESP TEXT,
        NO_NCM_POR TEXT,
        CO_CGCE_N3 INTEGER,
        NO_CGCE_N3 TEXT,
        NO_CGCE_N3_ING TEXT,
        NO_CGCE_N3_ESP TEXT,
        CO_CGCE_N2 INTEGER,
        NO_CGCE_N2 TEXT,
        NO_CGCE_N2_ING TEXT,
        NO_CGCE_N2_ESP TEXT,
        CO_CGCE_N1 INTEGER,
        NO_CGCE_N1 TEXT,
        NO_CGCE_N1_ING TEXT,
        NO_CGCE_N1_ESP TEXT,
        NO_NCM_POR_CO_CUCI TEXT,
        CO_CUCI_ITEM INTEGER,
        NO_CUCI_ITEM TEXT,
        CO_CUCI_SUB INTEGER,
        NO_CUCI_SUB TEXT,
        CO_CUCI_GRUPO INTEGER,
        NO_CUCI_GRUPO TEXT,
        CO_CUCI_DIVISAO INTEGER,
        NO_CUCI_DIVISAO TEXT,
        CO_CUCI_SEC INTEGER,
        NO_CUCI_SEC TEXT,
        NO_NCM_POR_CO_CUCI_GRUPO_CO_ISIC_SECAO TEXT,
        CO_ISIC_SECAO INTEGER,
        NO_ISIC_SECAO TEXT,
        CO_CUCI_GRUPO_CO_CUCI_GRUPO_CO_ISIC_SECAO TEXT,
        NO_CUCI_GRUPO_CO_CUCI_GRUPO_CO_ISIC_SECAO TEXT,
        NO_NCM_POR_CO_FAT_AGREG TEXT,
        CO_FAT_AGREG INTEGER,
        NO_FAT_AGREG TEXT,
        NO_FAT_AGREG_GP TEXT,
        NO_NCM_POR_CO_ISIC_CLASSE TEXT,
        CO_ISIC_CLASSE INTEGER,
        NO_ISIC_CLASSE TEXT,
        NO_ISIC_CLASSE_ING TEXT,
        NO_ISIC_CLASSE_ESP TEXT,
        CO_ISIC_GRUPO INTEGER,
        NO_ISIC_GRUPO TEXT,
        NO_ISIC_GRUPO_ING TEXT,
        NO_ISIC_GRUPO_ESP TEXT,
        CO_ISIC_DIVISAO INTEGER,
        NO_ISIC_DIVISAO TEXT,
        NO_ISIC_DIVISAO_ING TEXT,
        NO_ISIC_DIVISAO_ESP TEXT,
        CO_ISIC_SECAO_CO_ISIC_CLASSE TEXT,
        NO_ISIC_SECAO_CO_ISIC_CLASSE TEXT,
        NO_ISIC_SECAO_ING TEXT,
        NO_ISIC_SECAO_ESP TEXT,
        CO_PAIS_ISON3 INTEGER,
        CO_PAIS_ISOA3 TEXT,
        NO_PAIS TEXT,
        NO_PAIS_ING TEXT,
        NO_PAIS_ESP TEXT,
        NO_NCM_POR_CO_PPE TEXT,
        CO_PPE INTEGER,
        NO_PPE TEXT,
        NO_PPE_MIN TEXT,
        NO_PPE_ING TEXT,
        NO_NCM_POR_CO_PPI TEXT,
        CO_PPI INTEGER,
        NO_PPI TEXT,
        NO_PPI_MIN TEXT,
        NO_PPI_ING TEXT,
        NO_NCM_POR_CO_SH6 TEXT,
        CO_SH6 INTEGER,
        NO_SH6_POR TEXT,
        NO_SH6_ESP TEXT,
        NO_SH6_ING TEXT,
        CO_SH4 INTEGER,
        NO_SH4_POR TEXT,
        NO_SH4_ESP TEXT,
        NO_SH4_ING TEXT,
        CO_SH2 INTEGER,
        NO_SH2_POR TEXT,
        NO_SH2_ESP TEXT,
        NO_SH2_ING TEXT,
        CO_NCM_SECROM INTEGER,
        NO_SEC_POR TEXT,
        NO_SEC_ESP TEXT,
        NO_SEC_ING TEXT,
        NO_NCM_POR_CO_SIIT TEXT,
        CO_SIIT INTEGER,
        NO_SIIT TEXT,
        CO_UF INTEGER,
        SG_UF TEXT,
        NO_UF TEXT,
        NO_REGIAO TEXT,
        NO_NCM_POR_CO_UNID TEXT,
        CO_UNID_CO_UNID INTEGER,
        NO_UNID TEXT,
        SG_UNID TEXT,
        NO_URF TEXT,
        NO_VIA TEXT
    );'''
    conn.execute(sql)
    conn.commit()

def importar_csv_importacao(conn, csv_path):
    import pandas as pd
    print(f'Importando {csv_path} para tabela Importacao...')
    for chunk in pd.read_csv(csv_path, chunksize=50000):
        chunk.to_sql('Importacao', conn, if_exists='append', index=False)
    print('Importação do CSV concluída.')

if __name__ == '__main__':
    conn = sqlite3.connect(db_path)
    criar_tabela_importacao(conn)
    print('Pronto para importar o CSV de importadores. Execute importar_csv_importacao(conn, caminho_csv) quando desejar.')
    conn.close()
