import sys
import os

# --- EL PARCHE MÁGICO ---
# Esto le dice a Python: "Agrega la carpeta actual de trabajo a tu lista de búsqueda"
# Así podrá encontrar 'src' sin importar en qué subcarpeta esté este archivo.
sys.path.append(os.getcwd())
# ------------------------

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Ahora sí, Python encontrará esto sin llorar
from src.extract.extractor import DataExtractor
from src.transform.transformer import DataTransformer
from src.quality.validator import DataValidator

# Cargar entorno
load_dotenv()

def debug_cliente_load():
    print("🕵️‍♂️ INICIANDO DIAGNÓSTICO DE CARGA DE CLIENTES")
    
    # 1. Configurar Conexión DB
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(db_url)
        print("✅ Conexión a DB exitosa.")
    except Exception as e:
        print(f"❌ ERROR FATAL DE CONEXIÓN: {e}")
        return

    # 2. Preparar Datos
    # Usamos rutas absolutas basadas en donde estamos parados
    base_dir = os.getcwd()
    raw_dir = os.path.join(base_dir, "data", "raw")
    
    print("   1. Extrayendo...")
    extractor = DataExtractor()
    try:
        df = extractor.extract_csv(os.path.join(raw_dir, "clientes_raw.csv"))
    except FileNotFoundError:
        print("❌ ERROR: No encuentro el archivo clientes_raw.csv")
        return

    print("   2. Transformando...")
    transformer = DataTransformer()
    df = transformer.clean_clientes(df)
    
    print("   3. Validando...")
    validator = DataValidator()
    df_valid = validator.validate_clientes(df)
    
    print(f"   📦 Intentando cargar {len(df_valid)} clientes a PostgreSQL...")
    
    # 3. INTENTO DE CARGA (Verbose)
    try:
        df_valid.to_sql(
            "clientes", 
            con=engine, 
            if_exists='append', 
            index=False, 
            method='multi'
        )
        print("   🎉 ¡ÉXITO! Clientes cargados.")
    except Exception as e:
        print("\n❌❌❌ ERROR EN LA CARGA DE CLIENTES ❌❌❌")
        print(f"El error técnico es: {e}")
        print("-" * 30)
        # Si el error es de duplicados, es una buena señal (significa que conecta)

if __name__ == "__main__":
    debug_cliente_load()
