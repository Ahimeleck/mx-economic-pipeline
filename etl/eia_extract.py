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
TABLE = 'precio_wti'
EIA_API_KEY = os.getenv('EIA_API_KEY')
URL = 'https://api.eia.gov/v2/petroleum/pri/spt/data/'

def extract_eia():
    params = {
        'api_key': EIA_API_KEY,
        'frequency': 'daily',
        'data[0]': 'value',
        'facets[product][]': 'EPCWTI',
        'sort[0][column]': 'period',
        'sort[0][direction]': 'desc',
        'length': 30
    }
    
    response = requests.get(URL, params=params)
    data = response.json()
    registros = data['response']['data']
    
    df = pd.DataFrame(registros)
    df = df[['period', 'value']]
    df.columns = ['fecha', 'precio_wti']
    df['precio_wti'] = pd.to_numeric(df['precio_wti'], errors='coerce')
    df['fecha'] = pd.to_datetime(df['fecha'])
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
            bigquery.SchemaField("precio_wti", "FLOAT"),
            bigquery.SchemaField("fecha_carga", "TIMESTAMP"),
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Cargados {len(df)} registros a {table_id}")

if __name__ == '__main__':
    df = extract_eia()
    load_to_bigquery(df)