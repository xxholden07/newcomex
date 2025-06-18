import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import oracledb
import sqlite3
import socket
import time
import sys
import os
from datetime import datetime
import logging
import gc
from tqdm import tqdm

# Configuração de logging
def setup_logging():
    log_dir = 'Logs'
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'ml_importadores_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# Caminhos dos arquivos
DB_PATH = 'cnpj.db'
OUTPUT_PATH = 'Output_csv/possiveis_importadores_ml.csv'

# Variáveis de conexão Oracle
ORACLE_USER = 'MARKETING'
ORACLE_PASSWORD = 'MARKETING'
ORACLE_DSN = 'oxbrexacs-devhml.oxiteno.corp:1521/oxlabd.oxiteno.corp'

def check_vpn_connection():
    try:
        host = ORACLE_DSN.split(':')[0]
        socket.gethostbyname(host)
        logger.info("VPN conectada - Host Oracle acessível")
        return True
    except socket.gaierror:
        logger.error("VPN não conectada - Não foi possível resolver o host Oracle")
        return False

def connect_oracle(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            if not check_vpn_connection():
                raise ConnectionError("VPN não conectada")
            
            logger.info(f"Tentando conectar ao Oracle (tentativa {attempt + 1}/{max_retries})")
            conn = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN
            )
            logger.info("Conexão Oracle estabelecida com sucesso")
            return conn
        except Exception as e:
            logger.error(f"Erro na conexão Oracle: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Aguardando {retry_delay} segundos antes da próxima tentativa...")
                time.sleep(retry_delay)
            else:
                raise

def load_training_data():
    try:
        conn = connect_oracle()
        logger.info("Carregando dados de treino do LogComex...")

        logcomex = pd.read_sql('''
            SELECT NCM, UF_IMPORTADOR AS "UF IMPORTADOR", CIDADE_IMPORTADOR AS "CIDADE DO IMPORTADOR",
                   DESCRICAO_PRODUTO AS "Descrição produto", MODAL, PAIS_AQUISICAO AS "País de aquisição",
                   PESO_LIQUIDO AS "Peso líquido", VALOR_FOB_ESTIMADO_TOTAL AS "VALOR FOB ESTIMADO TOTAL",
                   VALOR_FRETE_TOTAL AS "Valor Frete total", VALOR_SEGURO_TOTAL AS "Valor Seguro total",
                   QTD_ESTATISTICA AS "QTD Estatística", UNIDADE_MEDIDA_ESTATISTICA AS "Unidade de Medida Estatística",
                   PROVAVEL_IMPORTADOR_CNPJ AS "PROVÁVEL IMPORTADOR CNPJ"
            FROM MARKETING.PUB_BR_LOGCOMEX
            WHERE PROVAVEL_IMPORTADOR_CNPJ IS NOT NULL
        ''', conn)

        logger.info(f"Dados de treino carregados: {len(logcomex)} registros")
        conn.close()
        return logcomex
    except Exception as e:
        logger.error(f"Erro ao carregar dados de treino: {str(e)}")
        raise

def train_model(X, y):
    try:
        logger.info("Iniciando treinamento do modelo...")
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X_train, y_train)

        score = clf.score(X_test, y_test)
        logger.info(f"Acurácia do modelo: {score:.2%}")
        return clf
    except Exception as e:
        logger.error(f"Erro no treinamento do modelo: {str(e)}")
        raise

def process_chunk(chunk):
    """Processa um chunk de dados cadastrais"""
    try:
        chunk['CNPJ_COMPLETO'] = chunk['CNPJ_BASICO'].astype(str).str.zfill(8) + \
            chunk['CNPJ_ORDEM'].fillna('').astype(str).str.zfill(4) + \
            chunk['CNPJ_DV'].fillna('').astype(str).str.zfill(2)
        return chunk
    except Exception as e:
        logger.error(f"Erro ao processar chunk: {str(e)}")
        return pd.DataFrame()

def processar_chunk(i, chunk_size, consulta_cnpj, conn_sqlite, importacao_chunk):
    """Processa um chunk de dados"""
    try:
        logger.info(f"Processando chunk {i//chunk_size + 1}")

        # Carrega chunk de cadastro
        chunk_query = f"{consulta_cnpj} LIMIT {chunk_size} OFFSET {i}"
        cadastro_chunk = pd.read_sql(chunk_query, conn_sqlite)
        
        if cadastro_chunk.empty:
            logger.warning(f"Chunk {i//chunk_size + 1} vazio")
            return pd.DataFrame()

        # Processa CNPJs
        cadastro_chunk = process_chunk(cadastro_chunk)
        
        # Merge completo
        merged_chunk = importacao_chunk.merge(
            cadastro_chunk,
            left_on='CNPJ_ML_NORMALIZADO',
            right_on='CNPJ_COMPLETO',
            how='left',
            suffixes=('', '_CNPJ')
        )

        # Merge básico para CNPJs não encontrados
        mask_missing = merged_chunk['CNPJ_BASICO'].isna()
        if mask_missing.any():
            merged_chunk_basico = importacao_chunk[mask_missing].merge(
                cadastro_chunk,
                left_on='CNPJ_ML_BASICO',
                right_on='CNPJ_BASICO',
                how='left',
                suffixes=('', '_CNPJ_BASICO')
            )
            
            # Atualiza colunas de forma segura
            for col in cadastro_chunk.columns:
                if col in merged_chunk_basico.columns:
                    merged_chunk.loc[mask_missing, col] = merged_chunk_basico[col].values
            
            merged_chunk.loc[mask_missing, 'ORIGEM_DADOS_CADASTRAIS'] = 'Receita Federal (CNPJ público, match parcial)'

        # Adiciona origens
        merged_chunk['ORIGEM_DADOS_CADASTRAIS'] = merged_chunk['ORIGEM_DADOS_CADASTRAIS'].fillna('Receita Federal (CNPJ público)')
        merged_chunk['ORIGEM_DADOS_IMPORTACAO'] = 'Siscomex/MDIC público'

        return merged_chunk

    except Exception as e:
        logger.error(f"Erro ao processar chunk {i//chunk_size + 1}: {str(e)}")
        return pd.DataFrame()

def main():
    try:
        if not check_vpn_connection():
            logger.error("Por favor, conecte à VPN antes de executar o script")
            return

        # Carrega e prepara dados de treino
        logcomex = load_training_data()
        features = ['NCM', 'UF IMPORTADOR', 'CIDADE DO IMPORTADOR', 'Descrição produto',
                   'MODAL', 'País de aquisição', 'Peso líquido', 'VALOR FOB ESTIMADO TOTAL',
                   'Valor Frete total', 'Valor Seguro total', 'QTD Estatística', 'Unidade de Medida Estatística']
        target = 'PROVÁVEL IMPORTADOR CNPJ'

        # Prepara dados para treinamento
        X = logcomex[features].fillna('')
        y = logcomex[target].astype(str)

        # Codifica variáveis
        encoders = {}
        for col in X.select_dtypes(include='object').columns:
            le = LabelEncoder()
            le.fit(X[col].astype(str))
            X[col] = le.transform(X[col].astype(str))
            encoders[col] = le

        le_target = LabelEncoder()
        y_enc = le_target.fit_transform(y)

        # Treina modelo
        clf = train_model(X, y_enc)

        # Carrega dados para predição
        conn = connect_oracle()
        importacao = pd.read_sql('''
            SELECT RECORD_KEY AS id, NCM, UF_IMPORTADOR AS "UF IMPORTADOR", CIDADE_IMPORTADOR AS "CIDADE DO IMPORTADOR",
                   DESCRICAO_PRODUTO AS "Descrição produto", MODAL, PAIS_AQUISICAO AS "País de aquisição",
                   PESO_LIQUIDO AS "Peso líquido", VALOR_FOB_ESTIMADO_TOTAL AS "VALOR FOB ESTIMADO TOTAL",
                   VALOR_FRETE_TOTAL AS "Valor Frete total", VALOR_SEGURO_TOTAL AS "Valor Seguro total",
                   QTD_ESTATISTICA AS "QTD Estatística", UNIDADE_MEDIDA_ESTATISTICA AS "Unidade de Medida Estatística"
            FROM MARKETING.PUB_BR_LOGCOMEX
        ''', conn)
        conn.close()

        # Prepara dados para predição
        X_pred = importacao[features].fillna('')
        for col in X_pred.select_dtypes(include='object').columns:
            le = encoders.get(col)
            if le:
                X_pred[col] = X_pred[col].map(lambda val: le.transform([val])[0] if val in le.classes_ else -1)
            else:
                X_pred[col] = 0

        # Faz predições
        importacao['PROVÁVEL IMPORTADOR CNPJ ML'] = le_target.inverse_transform(clf.predict(X_pred))
        importacao['ORIGEM_PROVÁVEL_IMPORTADOR'] = 'Inferido por ML (modelo treinado com LogComex, mas sem uso direto de dados)'

        # Limpa memória
        del X, y, X_pred, clf
        gc.collect()

        # Prepara dados para enriquecimento
        def normalizar_cnpj(cnpj):
            if pd.isna(cnpj):
                return ''
            return ''.join(filter(str.isdigit, str(cnpj))).zfill(14)

        importacao['CNPJ_ML_NORMALIZADO'] = importacao['PROVÁVEL IMPORTADOR CNPJ ML'].apply(normalizar_cnpj)
        importacao['CNPJ_ML_BASICO'] = importacao['CNPJ_ML_NORMALIZADO'].str[:8]

        # Configura processamento em chunks
        conn_sqlite = sqlite3.connect(DB_PATH)
        consulta_cnpj = open('consulta_unificada_cnpj.sql', encoding='utf-8').read().strip()
        if consulta_cnpj.endswith(';'):
            consulta_cnpj = consulta_cnpj[:-1]

        chunk_size = 25000  # Reduzido para melhor gerenciamento de memória
        try:
            total_rows = pd.read_sql(f"SELECT COUNT(*) as count FROM ({consulta_cnpj}) as subquery", conn_sqlite).iloc[0]['count']
            logger.info(f"Total de registros cadastrais: {total_rows}")
        except Exception as e:
            logger.error(f"Erro ao contar registros: {str(e)}")
            total_rows = 1000000
            logger.warning("Usando valor padrão de 1 milhão de registros")

        # Prepara tabela temporária
        conn_sqlite.execute("DROP TABLE IF EXISTS temp_importacao_enriquecida")
        importacao.to_sql('temp_importacao_enriquecida', conn_sqlite, if_exists='replace', index=False)

        # Processa chunks sequencialmente
        logger.info("Iniciando processamento dos chunks...")
        importacao_chunk = pd.read_sql("SELECT * FROM temp_importacao_enriquecida", conn_sqlite)
        
        # Processa em lotes menores
        batch_size = 5  # Número de chunks por lote
        for i in range(0, total_rows, chunk_size * batch_size):
            logger.info(f"Processando lote {i//(chunk_size * batch_size) + 1} de {(total_rows + chunk_size * batch_size - 1)//(chunk_size * batch_size)}")
            
            batch_results = []
            for j in range(batch_size):
                offset = i + (j * chunk_size)
                if offset >= total_rows:
                    break
                    
                result = processar_chunk(offset, chunk_size, consulta_cnpj, conn_sqlite, importacao_chunk)
                if not result.empty:
                    batch_results.append(result)
            
            # Concatena e salva resultados do lote
            if batch_results:
                batch_result = pd.concat(batch_results, ignore_index=True)
                if i == 0:
                    batch_result.to_sql('possiveis_importadores_ml_enriquecido', conn_sqlite, if_exists='replace', index=False)
                else:
                    batch_result.to_sql('possiveis_importadores_ml_enriquecido', conn_sqlite, if_exists='append', index=False)
            
            # Limpa memória
            del batch_results
            gc.collect()

        # Limpa tabela temporária
        conn_sqlite.execute("DROP TABLE IF EXISTS temp_importacao_enriquecida")
        conn_sqlite.close()
        
        logger.info("Processo concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro no processo principal: {str(e)}")
        raise

if __name__ == "__main__":
    main()
