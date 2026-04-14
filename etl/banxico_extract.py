import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import os

# Cargar variables de entorno
dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Configuración
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET = os.getenv('BQ_DATASET')
TABLE = 'tipo_cambio'
BANXICO_TOKEN = os.getenv('BANXICO_TOKEN')
URL = 'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos/oportuno'

def extract_banxico():
    headers = {'Bmx-Token': BANXICO_TOKEN}
    response = requests.get(URL, headers=headers)
    data = response.json()
    
    serie = data['bmx']['series'][0]
    datos = serie['datos']
    
    df = pd.DataFrame(datos)
    df.columns = ['fecha', 'tipo_cambio']
    df['tipo_cambio'] = pd.to_numeric(df['tipo_cambio'], errors='coerce')
    df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
    df['fecha_carga'] = pd.Timestamp.now()
    
    print(f"Registros extraídos: {len(df)}")
    return df

def load_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("fecha", "DATE"),
            bigquery.SchemaField("tipo_cambio", "FLOAT"),
            bigquery.SchemaField("fecha_carga", "TIMESTAMP"),
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Cargados {len(df)} registros a {table_id}")

if __name__ == '__main__':
    df = extract_banxico()
    load_to_bigquery(df)