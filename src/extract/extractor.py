import pandas as pd
import os
import json

class DataExtractor:
    """
    Módulo profesional para la ingesta de datos desde diversas fuentes.
    Implementa patrón Facade para unificar la lectura.
    """
    
    def __init__(self):
        # Definimos rutas base por si se necesitan logs específicos aquí
        pass

    def extract_csv(self, filepath: str) -> pd.DataFrame:
        """Lee archivos CSV con manejo de errores robusto."""
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"El archivo no existe: {filepath}")
            
            print(f"📥 Leyendo CSV: {filepath}")
            df = pd.read_csv(filepath)
            print(f"   ✔ Registros extraídos: {len(df)}")
            return df
        except Exception as e:
            print(f"   ❌ Error leyendo CSV: {e}")
            return pd.DataFrame() # Retorna vacío para no romper el pipeline

    def extract_excel(self, filepath: str) -> pd.DataFrame:
        """Lee archivos Excel (formato .xlsx)."""
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"El archivo no existe: {filepath}")
            
            print(f"📥 Leyendo Excel: {filepath}")
            # engine='openpyxl' es necesario para .xlsx modernos
            df = pd.read_excel(filepath, engine='openpyxl')
            print(f"   ✔ Registros extraídos: {len(df)}")
            return df
        except Exception as e:
            print(f"   ❌ Error leyendo Excel: {e}")
            return pd.DataFrame()

    def extract_json(self, filepath: str) -> pd.DataFrame:
        """Lee archivos JSON (lista de objetos)."""
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"El archivo no existe: {filepath}")
            
            print(f"📥 Leyendo JSON: {filepath}")
            df = pd.read_json(filepath)
            print(f"   ✔ Registros extraídos: {len(df)}")
            return df
        except Exception as e:
            print(f"   ❌ Error leyendo JSON: {e}")
            return pd.DataFrame()

# ---------------------------------------------------------
# BLOQUE DE PRUEBA (Main)
# Esto permite probar este archivo individualmente sin correr todo el proyecto
# ---------------------------------------------------------
if __name__ == "__main__":
    # Configuración de rutas para la prueba
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    
    extractor = DataExtractor()
    
    print("--- INICIANDO PRUEBA DE EXTRACCIÓN ---")
    
    # 1. Probar CSV
    df_clientes = extractor.extract_csv(os.path.join(RAW_DIR, "clientes_raw.csv"))
    print(f"   Muestra: {df_clientes.head(2).to_dict(orient='records')}\n")
    
    # 2. Probar Excel
    df_productos = extractor.extract_excel(os.path.join(RAW_DIR, "productos_master.xlsx"))
    print(f"   Muestra: {df_productos.head(2).to_dict(orient='records')}\n")

    # 3. Probar JSON
    df_tx = extractor.extract_json(os.path.join(RAW_DIR, "transacciones_raw.json"))
    print(f"   Muestra: {df_tx.head(2).to_dict(orient='records')}\n")
