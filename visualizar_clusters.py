import pandas as pd
import sqlite3
import sys

def visualizar_clusters():
    try:
        # Conecta ao banco de dados
        conn = sqlite3.connect('cnpj.db')
        
        # Verifica se a tabela existe
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='importadores_clusters'")
        if not cursor.fetchone():
            print("A tabela 'importadores_clusters' não existe no banco de dados.")
            return
        
        # Carrega os dados
        query = """
        SELECT 
            CLUSTER_PREDICTED,
            COUNT(*) as quantidade_importadores,
            AVG(PROBABILIDADE) as probabilidade_media,
            COUNT(DISTINCT "PROVÁVEL IMPORTADOR CNPJ") as cnpjs_unicos
        FROM importadores_clusters
        GROUP BY CLUSTER_PREDICTED
        ORDER BY CLUSTER_PREDICTED
        """
        
        df = pd.read_sql(query, conn)
        
        # Exibe o resumo
        print("\nResumo dos Clusters:")
        print(df.to_string(index=False))
        
        # Exibe detalhes de cada cluster
        print("\nDetalhes por Cluster:")
        for cluster in df['CLUSTER_PREDICTED'].unique():
            print(f"\nCluster {cluster}:")
            query = f"""
            SELECT 
                "PROVÁVEL IMPORTADOR CNPJ",
                "UF IMPORTADOR",
                "CIDADE DO IMPORTADOR",
                PROBABILIDADE
            FROM importadores_clusters
            WHERE CLUSTER_PREDICTED = {cluster}
            ORDER BY PROBABILIDADE DESC
            LIMIT 5
            """
            cluster_df = pd.read_sql(query, conn)
            print(cluster_df.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"Erro ao visualizar clusters: {str(e)}")

if __name__ == "__main__":
    visualizar_clusters() 