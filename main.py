from etl.banxico_extract import extract_banxico, load_to_bigquery as load_banxico
from etl.eia_extract import extract_eia, load_to_bigquery as load_eia

def run_pipeline():
    print("=== Iniciando pipeline mx-economic ===")
    
    print("\n--- Extrayendo tipo de cambio Banxico ---")
    df_banxico = extract_banxico()
    load_banxico(df_banxico)
    
    print("\n--- Extrayendo precio WTI EIA ---")
    df_eia = extract_eia()
    load_eia(df_eia)
    
    print("\n=== Pipeline completado exitosamente ===")

if __name__ == '__main__':
    run_pipeline()