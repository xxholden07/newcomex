import polars as pl
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.utils.class_weight import compute_class_weight
import oracledb
import sqlite3
import socket
import time
import sys
import os
from datetime import datetime
import logging
from tqdm import tqdm
import joblib
import warnings
warnings.filterwarnings('ignore')

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

# Configurações
DB_PATH = "C:/Users/MatheusRodrigues/Documents/Indorama/new_comex/cnpj.db"
ORACLE_USER = 'MARKETING'
ORACLE_PASSWORD = 'MARKETING'
ORACLE_DSN = 'oxbrexacs-devhml.oxiteno.corp:1521/oxlabd.oxiteno.corp'
MODEL_DIR = 'models'
os.makedirs(MODEL_DIR, exist_ok=True)

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
            conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
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
        query = '''
            SELECT 
                NCM, 
                UF_IMPORTADOR AS "UF IMPORTADOR", 
                CIDADE_IMPORTADOR AS "CIDADE DO IMPORTADOR",
                DESCRICAO_PRODUTO AS "Descrição produto", 
                MODAL, 
                PAIS_AQUISICAO AS "País de aquisição",
                PESO_LIQUIDO AS "Peso líquido", 
                VALOR_FOB_ESTIMADO_TOTAL AS "VALOR FOB ESTIMADO TOTAL",
                VALOR_FRETE_TOTAL AS "Valor Frete total", 
                VALOR_SEGURO_TOTAL AS "Valor Seguro total",
                QTD_ESTATISTICA AS "QTD Estatística", 
                UNIDADE_MEDIDA_ESTATISTICA AS "Unidade de Medida Estatística",
                PROVAVEL_IMPORTADOR_CNPJ AS "PROVÁVEL IMPORTADOR CNPJ"
            FROM MARKETING.PUB_BR_LOGCOMEX
            WHERE PROVAVEL_IMPORTADOR_CNPJ IS NOT NULL
        '''
        df = pl.read_database(query, conn)
        conn.close()
        logger.info(f"Dados de treino carregados: {df.shape[0]} registros")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados de treino: {str(e)}")
        raise

def preprocess_data(df: pl.DataFrame):
    logger.info("Pré-processando dados...")
    
    # Converter para pandas para processamento
    df_pd = df.to_pandas()
    
    # Definir colunas numéricas
    numeric_cols = ['Peso líquido', 'VALOR FOB ESTIMADO TOTAL', 'Valor Frete total', 
                   'Valor Seguro total', 'QTD Estatística']
    
    # Tratar valores nulos
    df_pd = df_pd.fillna({
        'NCM': '00000000',
        'UF IMPORTADOR': 'ND',
        'CIDADE DO IMPORTADOR': 'ND',
        'Descrição produto': '',
        'MODAL': 'ND',
        'País de aquisição': 'ND',
        'Peso líquido': 0,
        'VALOR FOB ESTIMADO TOTAL': 0,
        'Valor Frete total': 0,
        'Valor Seguro total': 0,
        'QTD Estatística': 0,
        'Unidade de Medida Estatística': 'ND'
    })
    
    # Normalizar valores numéricos
    scaler = StandardScaler()
    df_pd[numeric_cols] = scaler.fit_transform(df_pd[numeric_cols])
    
    return df_pd, scaler, numeric_cols

def encode_and_vectorize(df_pd: pd.DataFrame, target_column: str, text_column: str, numeric_cols: list):
    logger.info("Codificando e vetorizando variáveis...")
    
    # Codificar variáveis categóricas
    categorical_cols = ['NCM', 'UF IMPORTADOR', 'CIDADE DO IMPORTADOR', 
                       'MODAL', 'País de aquisição', 'Unidade de Medida Estatística']
    
    encoders = {}
    for col in categorical_cols:
        le = LabelEncoder()
        df_pd[col] = le.fit_transform(df_pd[col].astype(str))
        encoders[col] = le
    
    # Vetorizar texto
    tfidf = TfidfVectorizer(max_features=100, ngram_range=(1, 2))
    tfidf_matrix = tfidf.fit_transform(df_pd[text_column].fillna('').astype(str)).toarray()
    tfidf_df = pd.DataFrame(tfidf_matrix, 
                           columns=[f"tfidf_{i}" for i in range(tfidf_matrix.shape[1])])
    
    # Combinar features
    feature_cols = categorical_cols + numeric_cols
    X = pd.concat([df_pd[feature_cols], tfidf_df], axis=1)
    
    # Agrupar CNPJs em clusters baseados em características similares
    from sklearn.cluster import KMeans
    
    # Usar características numéricas para clustering
    cluster_features = df_pd[numeric_cols].copy()
    
    # Definir número fixo de clusters (9 para garantir classes de 0 a 8)
    n_clusters = 9
    
    # Normalizar features para clustering
    from sklearn.preprocessing import StandardScaler
    cluster_scaler = StandardScaler()
    cluster_features_scaled = cluster_scaler.fit_transform(cluster_features)
    
    # Aplicar K-means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(cluster_features_scaled)
    
    # Garantir que as classes sejam contínuas de 0 a n_clusters-1
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(cluster_labels)
    
    # Verificar distribuição das classes
    unique_classes, class_counts = np.unique(y, return_counts=True)
    logger.info(f"Distribuição das classes após clustering: {dict(zip(unique_classes, class_counts))}")
    
    # Calcular pesos das classes
    class_weights = compute_class_weight(
        class_weight='balanced',
        classes=np.unique(y),
        y=y
    )
    class_weight_dict = dict(zip(np.unique(y), class_weights))
    
    # Atualizar o kmeans com o scaler e label_encoder
    kmeans.scaler = cluster_scaler
    kmeans.label_encoder = label_encoder
    
    return X, y, encoders, kmeans, tfidf, feature_cols, class_weight_dict

def train_model(X, y, class_weight_dict):
    logger.info("Treinando modelo XGBoost...")
    
    # Verificar distribuição das classes
    unique_classes, class_counts = np.unique(y, return_counts=True)
    logger.info(f"Distribuição das classes: {dict(zip(unique_classes, class_counts))}")
    
    # Split dos dados
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Configurar e treinar modelo
    clf = XGBClassifier(
        use_label_encoder=False,
        eval_metric='mlogloss',
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        scale_pos_weight=1.0,  # Será ajustado pelo class_weight
        tree_method='hist'  # Método mais rápido e eficiente com memória
    )
    
    # Treinar modelo com pesos das classes
    clf.fit(
        X_train, 
        y_train,
        sample_weight=[class_weight_dict[y_i] for y_i in y_train]
    )
    
    # Avaliar modelo
    train_score = clf.score(X_train, y_train)
    test_score = clf.score(X_test, y_test)
    
    logger.info(f"Acurácia Treino: {train_score:.2%}")
    logger.info(f"Acurácia Teste: {test_score:.2%}")
    
    # Cross-validation
    cv_scores = cross_val_score(clf, X, y, cv=5)
    logger.info(f"Cross-validation scores: {cv_scores.mean():.2%} (+/- {cv_scores.std() * 2:.2%})")
    
    # Relatório de classificação
    y_pred = clf.predict(X_test)
    logger.info("\nRelatório de Classificação:")
    logger.info(classification_report(y_test, y_pred))
    
    return clf, X_test, y_test

def save_model_components(clf, encoders, kmeans, tfidf, scaler, feature_cols, class_weight_dict):
    logger.info("Salvando componentes do modelo...")
    
    components = {
        'model': clf,
        'encoders': encoders,
        'kmeans': kmeans,
        'tfidf': tfidf,
        'scaler': scaler,
        'feature_cols': feature_cols,
        'class_weight_dict': class_weight_dict
    }
    
    joblib.dump(components, os.path.join(MODEL_DIR, 'modelo_importadores.joblib'))
    logger.info("Componentes do modelo salvos com sucesso")

def save_results_to_db(df, predictions, cluster_labels):
    """Salva os resultados no banco de dados SQLite"""
    try:
        logger.info("Salvando resultados no banco de dados...")
        
        # Conecta ao banco SQLite
        conn = sqlite3.connect(DB_PATH)
        
        # Prepara o DataFrame com os resultados
        results_df = df.copy()
        results_df['CLUSTER_PREDICTED'] = cluster_labels
        results_df['PROBABILIDADE'] = predictions
        
        # Salva no banco de dados
        results_df.to_sql('importadores_clusters', conn, if_exists='replace', index=False)
        
        # Cria índices para melhor performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cluster ON importadores_clusters(CLUSTER_PREDICTED)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cnpj ON importadores_clusters("PROVÁVEL IMPORTADOR CNPJ")')
        
        conn.close()
        logger.info("Resultados salvos com sucesso no banco de dados")
        
    except Exception as e:
        logger.error(f"Erro ao salvar resultados no banco de dados: {str(e)}")
        raise

def main():
    try:
        # Carregar dados
        df = load_training_data()
        
        # Pré-processar dados
        df_pd, scaler, numeric_cols = preprocess_data(df)
        
        # Codificar e vetorizar
        X, y, encoders, kmeans, tfidf, feature_cols, class_weight_dict = encode_and_vectorize(
            df_pd, 
            'PROVÁVEL IMPORTADOR CNPJ',
            'Descrição produto',
            numeric_cols
        )
        
        # Treinar modelo
        clf, X_test, y_test = train_model(X, y, class_weight_dict)
        
        # Salvar componentes do modelo
        save_model_components(clf, encoders, kmeans, tfidf, scaler, feature_cols, class_weight_dict)
        
        # Fazer previsões para todos os dados
        predictions = clf.predict_proba(X)
        
        # Usar apenas as features numéricas para o K-means
        numeric_features_scaled = scaler.transform(df_pd[numeric_cols])
        cluster_labels = kmeans.predict(numeric_features_scaled)
        
        # Salvar resultados no banco de dados
        save_results_to_db(df_pd, predictions.max(axis=1), cluster_labels)
        
        logger.info("Processo concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro no processo principal: {str(e)}")
        raise

if __name__ == "__main__":
    main() 