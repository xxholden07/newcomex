import os
import time
import pandas as pd
import urllib3
from datetime import datetime
import logging
import sys
import glob

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'Logs/comex_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def carregar_dados_importacao_json():
    """
    Carrega os dados de importação a partir do arquivo JSON no formato NDJSON.
    """
    try:
        logging.info("Procurando arquivos de importação...")
        
        # Procura por arquivos JSON na pasta Input
        input_dir = os.path.join(os.path.dirname(__file__), 'Input')
        json_files = glob.glob(os.path.join(input_dir, '**', '*.json'), recursive=True)
        
        if not json_files:
            logging.error("Nenhum arquivo JSON encontrado na pasta Input")
            return None
            
        # Usa o arquivo mais recente
        latest_file = max(json_files, key=os.path.getctime)
        logging.info(f"Arquivo selecionado: {latest_file}")
        
        df = pd.read_json(latest_file, lines=True)
        logging.info(f"Dados carregados com sucesso. Total de registros: {len(df)}")
        logging.info(f"Colunas do JSON: {list(df.columns)}")
        return df
    except Exception as e:
        logging.error(f"Erro ao carregar dados de importação JSON: {str(e)}")
        return None

def carregar_tabelas_auxiliares():
    """
    Carrega todas as tabelas auxiliares a partir dos arquivos JSON na pasta tabs_aux_json.
    Retorna um dicionário com os DataFrames de cada tabela.
    """
    try:
        logging.info("Carregando tabelas auxiliares dos arquivos JSON...")
        tabelas = {}
        pasta_json = os.path.join(os.path.dirname(__file__), 'tabs_aux_json')
        
        if not os.path.exists(pasta_json):
            logging.error(f"Pasta de tabelas auxiliares não encontrada: {pasta_json}")
            return None
            
        arquivos_json = [f for f in os.listdir(pasta_json) if f.endswith('.json')]
        
        if not arquivos_json:
            logging.error("Nenhum arquivo JSON encontrado na pasta de tabelas auxiliares")
            return None

        for arquivo in arquivos_json:
            nome = arquivo.replace('.json', '')
            caminho_arquivo = os.path.join(pasta_json, arquivo)
            try:
                df = pd.read_json(caminho_arquivo, orient='records')
                logging.info(f"Colunas da tabela {nome}: {list(df.columns)}")
                tabelas[nome] = {'df': df}
                logging.info(f"Tabela {nome} carregada com sucesso: {len(df)} registros")
            except Exception as e:
                logging.warning(f"Erro ao carregar {arquivo}: {str(e)}")
                continue
                
        if not tabelas:
            logging.error("Nenhuma tabela auxiliar foi carregada com sucesso")
            return None
            
        return tabelas
    except Exception as e:
        logging.error(f"Erro ao carregar tabelas auxiliares dos JSONs: {str(e)}")
        return None

def enriquecer_dados(df, tabelas_auxiliares):
    """
    Enriquece os dados principais com tabelas auxiliares, usando chaves de merge seguras e específicas para cada tabela.
    """
    if tabelas_auxiliares is None:
        logging.warning("Nenhuma tabela auxiliar disponível para enriquecimento")
        return df

    logging.info("Enriquecendo dados com tabelas auxiliares relevantes...")
    try:
        # Filtrar apenas os NCMs desejados antes do enriquecimento
        ncms_filtrar = [
            29314914,29309098,29241999,29241929,29239090,29221500,29221200,29221100,29212990,29211999,29211121,29161410,29161310,29156019,29154020,29154010,29153999,29153939,29153932,29153931,29153929,29153300,29153100,29152100,29141200,29126000,29101000,29094939,29094932,29094921,29094421,29094419,29094411,29094320,29094310,29094100,29091990,29072300,29071990,29071300,29071100,29054500,29054400,29053200,29053100,29051996,29051730,29051720,29051600,29051420,29051410,29051300,29051210,28152000,28141000,27121000,27101999,27101991,27101931,15162000,15153000,15132919,59022000,59021090,59021010,54071019,54022090,54021910,39076100,39072939,39072931,39072912,39069019,39069011,39061000,38249989,38249929,38249923,38246000,38237090,38237020,38237010,38231990,38231300,38231200,38231100,38190000,38089324,38089299,38040020,34042010,34029090,34029029,34024200,34024190,34023990,34023100
        ]
        df = df[df['CO_NCM'].astype(str).isin([str(ncm) for ncm in ncms_filtrar])].copy()
        logging.info(f"Filtrado para {len(df)} registros com NCMs desejados.")

        # Mapeamento manual de chaves de merge seguras
        mapeamento_merges = {
            'CO_SH6':        {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_CGCE_N3':    {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_CUCI':       {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_CUCI_GRUPO_CO_ISIC_SECAO': {'chave_principal': 'CO_NCM', 'chave_aux': 'CO_NCM'},
            'CO_FAT_AGREG':  {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_ISIC_CLASSE':{'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_PPE':        {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_PPI':        {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_SIIT':       {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_UNID':       {'chave_principal': 'CO_NCM',      'chave_aux': 'CO_NCM'},
            'CO_PAIS':       {'chave_principal': 'CO_PAIS',     'chave_aux': 'CO_PAIS'},
            'CO_BLOCO':      {'chave_principal': 'CO_PAIS',     'chave_aux': 'CO_PAIS'},
            'CO_UF':         {'chave_principal': 'SG_UF_NCM',   'chave_aux': 'SG_UF'},
            'CO_URF':        {'chave_principal': 'CO_URF',      'chave_aux': 'CO_URF'},
            'CO_VIA':        {'chave_principal': 'CO_VIA',      'chave_aux': 'CO_VIA'},
            'CO_MUN_GEO':    {'chave_principal': 'CO_MUN_GEO',  'chave_aux': 'CO_MUN_GEO'},
            'CO_MUN_GEO_NCM':{'chave_principal': 'CO_MUN_GEO',  'chave_aux': 'CO_MUN_GEO'},
        }

        for nome, info in tabelas_auxiliares.items():
            aux_df = info['df']
            if nome in mapeamento_merges:
                chave_principal = mapeamento_merges[nome]['chave_principal']
                chave_aux = mapeamento_merges[nome]['chave_aux']
                if chave_principal in df.columns and chave_aux in aux_df.columns:
                    logging.info(f"Fazendo merge com {nome} pela chave: {chave_principal} <-> {chave_aux}")
                    df = df.merge(aux_df, left_on=chave_principal, right_on=chave_aux, how='left', suffixes=('', f'_{nome}'))
                else:
                    logging.warning(f"Chave(s) não encontrada(s) para merge com {nome}: {chave_principal} ou {chave_aux}")
            else:
                logging.info(f"Tabela {nome} ignorada (sem mapeamento de merge seguro)")
        logging.info("Dados enriquecidos com tabelas auxiliares relevantes!")
        return df
    except Exception as e:
        logging.error(f"Erro ao enriquecer dados: {str(e)}")
        return df

def processar_e_salvar_dados(df):
    """
    Processa e salva os dados coletados.
    """
    try:
        if df is None or df.empty:
            logging.error("DataFrame vazio ou None, não é possível processar")
            return
            
        logging.info("Analisando agrupamento por país...")
        # Agrupar por CO_PAIS
        importadores = df.groupby('CO_PAIS').agg({
            'VL_FOB': 'sum',
            'KG_LIQUIDO': 'sum',
            'CO_NCM': 'nunique'
        }).reset_index()

        importadores.columns = ['CO_PAIS', 'valor_total_fob', 'quantidade_total_kg', 'num_ncms']
        importadores = importadores.sort_values('valor_total_fob', ascending=False)

        # Criar diretório de saída se não existir
        output_dir = os.path.join(os.path.dirname(__file__), 'Output_csv')
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_importadores = os.path.join(output_dir, f"indovinya_importadores_ranking_{timestamp}.csv")
        output_csv = os.path.join(output_dir, f"indovinya_importadores_{timestamp}.csv")
        output_parquet = os.path.join(output_dir, f"indovinya_importadores_{timestamp}.parquet")

        importadores.to_csv(output_importadores, index=False)
        df.to_csv(output_csv, index=False)
        df.to_parquet(output_parquet, index=False)

        logging.info(f"Arquivos salvos com sucesso:\n - {output_importadores}\n - {output_csv}\n - {output_parquet}")
    except Exception as e:
        logging.error(f"Erro ao processar e salvar dados: {str(e)}")
        raise

def main():
    try:
        # Criar diretório de logs se não existir
        os.makedirs('Logs', exist_ok=True)
        
        tabelas_auxiliares = carregar_tabelas_auxiliares()
        if tabelas_auxiliares is None:
            raise Exception("Não foi possível carregar as tabelas auxiliares")

        df = carregar_dados_importacao_json()
        if df is None:
            raise Exception("Não foi possível carregar os dados de importação")

        df_enriquecido = enriquecer_dados(df, tabelas_auxiliares)
        processar_e_salvar_dados(df_enriquecido)

    except Exception as e:
        logging.error(f"Erro na execução principal: {str(e)}")
        raise

if __name__ == "__main__":
    main()