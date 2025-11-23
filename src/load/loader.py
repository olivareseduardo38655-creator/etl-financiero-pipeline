import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

class DataLoader:
    def __init__(self):
        load_dotenv()
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.dbname = os.getenv("DB_NAME")
        
        self.db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        
        try:
            self.engine = create_engine(self.db_url)
            print(f"🔌 Motor SQL inicializado correctamente hacia: {self.dbname}")
        except Exception as e:
            print(f"❌ Error creando motor SQL: {e}")

    def clean_tables(self):
        print("🧹 Limpiando base de datos (TRUNCATE)...")
        try:
            with self.engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE transacciones, clientes, productos_financieros CASCADE;"))
                conn.commit()
            print("   ✔ Tablas vaciadas correctamente.")
        except Exception as e:
            print(f"   ❌ Error limpiando tablas: {e}")

    def get_valid_ids_from_db(self, table_name: str, id_column: str) -> list:
        """
        Consulta a la base de datos para ver qué IDs REALMENTE se guardaron.
        Esta es la 'Fuente de la Verdad'.
        """
        try:
            query = f"SELECT {id_column} FROM {table_name}"
            df_db = pd.read_sql(query, self.engine)
            # Convertimos a lista de strings limpios para comparar
            return df_db[id_column].astype(str).str.strip().tolist()
        except Exception as e:
            print(f"❌ Error obteniendo IDs de {table_name}: {e}")
            return []

    def load_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        try:
            if df.empty:
                print(f"⚠️  No hay datos para cargar en {table_name}.")
                return

            print(f"🚀 Cargando {len(df)} registros en tabla '{table_name}'...")
            
            df.to_sql(
                table_name, 
                con=self.engine, 
                if_exists=if_exists, 
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"   ✅ Carga completada en '{table_name}'.")
            
        except Exception as e:
            print(f"   ❌ Error CRÍTICO en carga SQL ({table_name}):")
            print(f"      Detalle: {e}")

# ---------------------------------------------------------
# BLOQUE DE EJECUCIÓN
# ---------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from src.extract.extractor import DataExtractor
    from src.transform.transformer import DataTransformer
    from src.quality.validator import DataValidator
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    
    extractor = DataExtractor()
    transformer = DataTransformer()
    validator = DataValidator()
    loader = DataLoader()
    
    print("--- TEST DE CARGA FINAL (ESTRATEGIA: SOURCE OF TRUTH) ---")
    
    # 0. LIMPIEZA
    loader.clean_tables()
    
    # 1. PROCESO CLIENTES
    print("\n🔹 1. Procesando Clientes...")
    df_cli_raw = extractor.extract_csv(os.path.join(RAW_DIR, "clientes_raw.csv"))
    df_cli_clean = transformer.clean_clientes(df_cli_raw)
    df_cli_valid = validator.validate_clientes(df_cli_clean)
    
    loader.load_data(df_cli_valid, "clientes")
    
    # 2. PROCESO PRODUCTOS
    print("\n🔹 2. Procesando Productos...")
    df_prod_raw = extractor.extract_excel(os.path.join(RAW_DIR, "productos_master.xlsx"))
    df_prod_clean = transformer.clean_productos(df_prod_raw)
    cols_db = ['producto_id', 'nombre_producto', 'tipo']
    df_prod_final = df_prod_clean[cols_db]
    
    loader.load_data(df_prod_final, "productos_financieros")
    
    # --- PUNTO CLAVE: Obtener la verdad desde la Base de Datos ---
    print("\n🔍 Consultando Base de Datos para integridad...")
    real_db_clients = loader.get_valid_ids_from_db("clientes", "cliente_id")
    real_db_products = loader.get_valid_ids_from_db("productos_financieros", "producto_id")
    
    print(f"   ℹ️  Clientes confirmados en DB: {len(real_db_clients)}")
    print(f"   ℹ️  Productos confirmados en DB: {len(real_db_products)}")

    # 3. PROCESO TRANSACCIONES
    print("\n🔹 3. Procesando Transacciones...")
    df_tx_raw = extractor.extract_json(os.path.join(RAW_DIR, "transacciones_raw.json"))
    df_tx_clean = transformer.clean_transacciones(df_tx_raw)
    df_tx_valid_quality = validator.validate_transacciones(df_tx_clean)
    
    # --- FILTRO INFALIBLE ---
    # Filtramos usando la lista que vino de la DB, no la de memoria.
    print("   🛡️  Filtrando transacciones contra registros reales de DB...")
    
    mask_client_ok = df_tx_valid_quality['cliente_id'].astype(str).isin(real_db_clients)
    mask_prod_ok = df_tx_valid_quality['producto_id'].astype(str).isin(real_db_products)
    
    df_tx_final_load = df_tx_valid_quality[mask_client_ok & mask_prod_ok]
    
    n_dropped = len(df_tx_valid_quality) - len(df_tx_final_load)
    if n_dropped > 0:
        print(f"   ⚠️  Se eliminaron {n_dropped} transacciones huérfanas.")
    
    # CARGA FINAL
    loader.load_data(df_tx_final_load, "transacciones")
