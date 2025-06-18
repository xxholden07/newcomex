import sqlite3
from import_parquet_polars import importar_csv_importacao

if __name__ == '__main__':
    conn = sqlite3.connect('cnpj.db')
    importar_csv_importacao(conn, r'Output_csv/indovinya_importadores_20250616_152004.csv')
    conn.close()
    print('Importação do CSV concluída.')
