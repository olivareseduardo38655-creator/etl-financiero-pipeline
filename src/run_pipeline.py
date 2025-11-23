import os
import sys

# --- FIX CRÍTICO DE RUTA (Path Hack) ---
# Esto obliga a Python a mirar la carpeta "padre" (la raíz del proyecto)
# Así podrá encontrar el módulo 'src' correctamente.
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# ---------------------------------------

import time
import logging
from datetime import datetime

# Configuración de Logging (Blindada para Windows)
log_dir = os.path.join(parent_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

# Configuración manual del logger para asegurar UTF-8 en el archivo
# y evitar errores de emojis en consolas antiguas
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler 1: Archivo (Con UTF-8 forzado para soportar emojis)
file_handler = logging.FileHandler(os.path.join(log_dir, log_filename), encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Handler 2: Consola (Stream)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

def run_pipeline():
    # Usamos texto simple en logs de consola por si acaso Windows se queja,
    # pero mantenemos la estructura profesional.
    logger.info(">>> INICIANDO PIPELINE ETL FINANCIERO MASTER")
    start_time = time.time()
    
    try:
        # -----------------------------------------------------
        # 1. SETUP DE MÓDULOS (Ahora sí funcionará el import)
        # -----------------------------------------------------
        from src.extract.extractor import DataExtractor
        from src.transform.transformer import DataTransformer
        from src.quality.validator import DataValidator
        from src.load.loader import DataLoader
        
        # Rutas (Usando parent_dir que calculamos arriba)
        RAW_DIR = os.path.join(parent_dir, "data", "raw")

        # Instancias
        extractor = DataExtractor()
        transformer = DataTransformer()
        validator = DataValidator()
        loader = DataLoader()

        # -----------------------------------------------------
        # 2. PREPARACIÓN
        # -----------------------------------------------------
        logger.info("--- FASE 0: PREPARACION DE BASE DE DATOS ---")
        loader.clean_tables()

        # -----------------------------------------------------
        # 3. PROCESAMIENTO DE CLIENTES
        # -----------------------------------------------------
        logger.info("--- FASE 1: PROCESANDO CLIENTES ---")
        df_cli = extractor.extract_csv(os.path.join(RAW_DIR, "clientes_raw.csv"))
        if not df_cli.empty:
            df_cli = transformer.clean_clientes(df_cli)
            df_cli = validator.validate_clientes(df_cli)
            loader.load_data(df_cli, "clientes")
        
        # -----------------------------------------------------
        # 4. PROCESAMIENTO DE PRODUCTOS
        # -----------------------------------------------------
        logger.info("--- FASE 2: PROCESANDO PRODUCTOS ---")
        df_prod = extractor.extract_excel(os.path.join(RAW_DIR, "productos_master.xlsx"))
        if not df_prod.empty:
            df_prod = transformer.clean_productos(df_prod)
            df_prod = df_prod[['producto_id', 'nombre_producto', 'tipo']]
            loader.load_data(df_prod, "productos_financieros")

        # -----------------------------------------------------
        # 5. VERIFICACIÓN DE INTEGRIDAD
        # -----------------------------------------------------
        logger.info("--- FASE 3: VERIFICACION DE INTEGRIDAD ---")
        valid_clients = loader.get_valid_ids_from_db("clientes", "cliente_id")
        valid_products = loader.get_valid_ids_from_db("productos_financieros", "producto_id")
        logger.info(f"Snapshot DB -> Clientes: {len(valid_clients)} | Productos: {len(valid_products)}")

        # -----------------------------------------------------
        # 6. PROCESAMIENTO DE TRANSACCIONES
        # -----------------------------------------------------
        logger.info("--- FASE 4: PROCESANDO TRANSACCIONES ---")
        df_tx = extractor.extract_json(os.path.join(RAW_DIR, "transacciones_raw.json"))
        
        if not df_tx.empty:
            df_tx = transformer.clean_transacciones(df_tx)
            df_tx = validator.validate_transacciones(df_tx)
            
            # Filtro cruzado contra DB
            mask_cli = df_tx['cliente_id'].astype(str).isin(valid_clients)
            mask_prod = df_tx['producto_id'].astype(str).isin(valid_products)
            
            df_final = df_tx[mask_cli & mask_prod]
            n_dropped = len(df_tx) - len(df_final)
            
            if n_dropped > 0:
                logger.warning(f"ALERTA: Se descartaron {n_dropped} transacciones por integridad referencial.")
            
            loader.load_data(df_final, "transacciones")

        elapsed = time.time() - start_time
        logger.info(f"FIN DEL PROCESO. TIEMPO TOTAL: {elapsed:.2f} SEGUNDOS")

    except Exception as e:
        logger.error(f"ERROR CRITICO EN EL PIPELINE: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
