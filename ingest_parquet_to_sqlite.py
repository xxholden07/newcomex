import os
import sqlite3
import polars as pl
import logging
from datetime import datetime
import traceback
import glob

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'Logs/ingest_sqlite_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Configurações
PARQUET_DIR = 'Input/dados_parquet'
DB_PATH = 'cnpj.db'

# Dicionário de nomes de colunas para cada tabela
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
    'Empresas': sorted(glob.glob(os.path.join(PARQUET_DIR, 'Empresas*_EMPRECSV.parquet'))),
    'Estabelecimentos': sorted(glob.glob(os.path.join(PARQUET_DIR, 'Estabelecimentos*_ESTABELE.parquet'))),
    'Simples': [os.path.join(PARQUET_DIR, 'Simples_D50510.parquet')],
    'Socios': sorted(glob.glob(os.path.join(PARQUET_DIR, 'Socios*_SOCIOCSV.parquet'))),
    'Paises': [os.path.join(PARQUET_DIR, 'Paises_PAISCSV.parquet')],
    'Municipios': [os.path.join(PARQUET_DIR, 'Municipios_MUNICCSV.parquet')],
    'Qualificacoes': [os.path.join(PARQUET_DIR, 'Qualificacoes_QUALSCSV.parquet')],
    'Naturezas': [os.path.join(PARQUET_DIR, 'Naturezas_NATJUCSV.parquet')],
    'Cnaes': [os.path.join(PARQUET_DIR, 'Cnaes_CNAECSV.parquet')],
    'Motivos': [os.path.join(PARQUET_DIR, 'Motivos_MOTICSV.parquet')]
}

# Configurações de processamento em lote
BATCH_SIZES = {
    'Empresas': 100000,
    'Estabelecimentos': 100000,
    'Simples': 100000,
    'Socios': 100000,
    'Paises': 1000,
    'Municipios': 1000,
    'Qualificacoes': 1000,
    'Naturezas': 1000,
    'Cnaes': 1000,
    'Motivos': 1000
}

def apagar_tabelas(conn):
    """Apaga todas as tabelas existentes no banco de dados"""
    try:
        logging.info("Apagando tabelas existentes...")
        cursor = conn.cursor()
        
        # Lista de todas as tabelas
        tabelas = [
            'Empresas', 'Estabelecimentos', 'Simples', 'Socios',
            'Paises', 'Municipios', 'Qualificacoes', 'Naturezas',
            'Cnaes', 'Motivos'
        ]
        
        # Desativa as chaves estrangeiras temporariamente
        cursor.execute("PRAGMA foreign_keys = OFF;")
        
        # Apaga cada tabela se existir
        for tabela in tabelas:
            cursor.execute(f"DROP TABLE IF EXISTS {tabela};")
            logging.info(f"Tabela {tabela} apagada (se existia)")
        
        # Reativa as chaves estrangeiras
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        conn.commit()
        logging.info("Todas as tabelas foram apagadas com sucesso")
    except Exception as e:
        logging.error(f"Erro ao apagar tabelas: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

def criar_tabelas(conn):
    """Cria as tabelas no banco de dados"""
    try:
        with open('create_cnpj_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
            conn.executescript(sql_script)
        conn.commit()
        logging.info("Tabelas criadas com sucesso")
    except Exception as e:
        logging.error(f"Erro ao criar tabelas: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

def processar_arquivo_parquet(arquivo, tabela, conn):
    """Processa um arquivo parquet e insere os dados no banco SQLite"""
    try:
        logging.info(f"Processando arquivo {arquivo} para tabela {tabela}")
        
        # Ler arquivo parquet
        df = pl.read_parquet(arquivo)
        df.columns = colunas_tabelas[tabela]
        total_rows = len(df)
        logging.info(f"Total de registros no arquivo: {total_rows}")
        
        # Processar em lotes
        batch_size = BATCH_SIZES[tabela]
        for i in range(0, total_rows, batch_size):
            batch = df.slice(i, batch_size)
            batch_pd = batch.to_pandas()
            
            # Inserir lote no banco
            batch_pd.to_sql(tabela, conn, if_exists='append', index=False)
            
            logging.info(f"Processados {min(i + batch_size, total_rows)} de {total_rows} registros")
        
        logging.info(f"Arquivo {arquivo} processado com sucesso")
        return True
    except Exception as e:
        logging.error(f"Erro ao processar arquivo {arquivo}: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    """Função principal que executa o processo de ingestão"""
    try:
        start_time = datetime.now()
        logging.info("Iniciando processo de ingestão...")
        
        # Conectar ao banco de dados
        conn = sqlite3.connect(DB_PATH)
        
        # Apagar tabelas existentes
        apagar_tabelas(conn)
        
        # Criar tabelas
        criar_tabelas(conn)
        
        # Ordem de processamento das tabelas
        ordem_tabelas = [
            'Paises', 'Municipios', 'Qualificacoes', 'Naturezas', 'Cnaes', 'Motivos',  # Tabelas pequenas primeiro
            'Empresas', 'Estabelecimentos', 'Simples', 'Socios'  # Tabelas grandes depois
        ]
        
        # Processar cada tabela
        for tabela in ordem_tabelas:
            if tabela not in arquivos_tabelas:
                logging.warning(f"Tabela {tabela} não encontrada no mapeamento de arquivos")
                continue
                
            arquivos = arquivos_tabelas[tabela]
            if not arquivos:
                logging.warning(f"Nenhum arquivo encontrado para a tabela {tabela}")
                continue
            
            logging.info(f"\n{'='*50}")
            logging.info(f"Iniciando processamento da tabela: {tabela}")
            logging.info(f"Total de arquivos para processar: {len(arquivos)}")
            
            # Processar cada arquivo da tabela
            for arquivo in arquivos:
                sucesso = processar_arquivo_parquet(arquivo, tabela, conn)
                if not sucesso:
                    logging.error(f"Falha ao processar arquivo {arquivo}")
                    continue
            
            logging.info(f"Tabela {tabela} processada com sucesso")
        
        # Fechar conexão
        conn.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"\nProcesso de ingestão concluído em {duration:.2f} segundos")
        
    except Exception as e:
        logging.error(f"Erro na execução do processo de ingestão: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main() 