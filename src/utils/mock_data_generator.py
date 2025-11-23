import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

def generate_mock_data():
    """
    Genera datos sintéticos para simular un entorno financiero real.
    Incluye datos sucios intencionalmente para probar el módulo de Data Quality.
    """
    print(f"⚙️ Generando datos en: {RAW_DIR}")
    
    # ---------------------------------------------------------
    # 1. CLIENTES (CSV)
    # ---------------------------------------------------------
    n_clientes = 100
    ids = [f"C{str(i).zfill(4)}" for i in range(1, n_clientes + 1)]
    
    data_clientes = {
        "cliente_id": ids,
        "nombre": [f"Cliente_{i}" for i in range(1, n_clientes + 1)],
        "email": [f"cliente{i}@email.com" for i in range(1, n_clientes + 1)],
        "fecha_registro": pd.date_range(start="2023-01-01", periods=n_clientes, freq="D"),
        "segmento": np.random.choice(["PREMIUM", "REGULAR", "JUNIOR"], n_clientes)
    }
    
    df_clientes = pd.DataFrame(data_clientes)
    
    # INYECTAR SUCIEDAD (Data Quality Tests)
    # Duplicamos un ID
    df_clientes.loc[5, "cliente_id"] = "C0001" 
    # Un email inválido
    df_clientes.loc[10, "email"] = "email_sin_arroba.com"
    # Un nulo
    df_clientes.loc[15, "nombre"] = None
    
    csv_path = os.path.join(RAW_DIR, "clientes_raw.csv")
    df_clientes.to_csv(csv_path, index=False)
    print(f"✅ Generado: {csv_path} (con errores intencionales)")

    # ---------------------------------------------------------
    # 2. PRODUCTOS (EXCEL)
    # ---------------------------------------------------------
    # Simulamos un archivo maestro que vendría de un área de negocio
    data_productos = {
        "producto_id": ["P01", "P02", "P03", "P04"],
        "nombre_producto": ["Cuenta Ahorro", "Tarjeta Oro", "Hipoteca", "Inversión 365"],
        "tipo": ["DEBITO", "CREDITO", "CREDITO", "INVERSION"],
        "tasa_interes": [0.01, 0.45, 0.12, 0.11] # Columna extra que no va a DB (para probar limpieza)
    }
    df_productos = pd.DataFrame(data_productos)
    
    excel_path = os.path.join(RAW_DIR, "productos_master.xlsx")
    df_productos.to_excel(excel_path, index=False)
    print(f"✅ Generado: {excel_path}")

    # ---------------------------------------------------------
    # 3. TRANSACCIONES (JSON)
    # ---------------------------------------------------------
    n_tx = 500
    
    data_tx = {
        "transaccion_id": [f"TX{str(i).zfill(6)}" for i in range(1, n_tx + 1)],
        "cliente_id": np.random.choice(ids, n_tx),
        "producto_id": np.random.choice(["P01", "P02", "P03"], n_tx),
        "monto": np.round(np.random.uniform(10.0, 5000.0, n_tx), 2),
        "fecha_transaccion": pd.date_range(start="2024-01-01", periods=n_tx, freq="H").astype(str),
        "tipo_movimiento": np.random.choice(["ENTRADA", "SALIDA"], n_tx)
    }
    
    df_tx = pd.DataFrame(data_tx)
    
    # INYECTAR ERROR LÓGICO
    # Monto negativo (no permitido por reglas de negocio si no es un ajuste)
    df_tx.loc[0, "monto"] = -500.00
    
    json_path = os.path.join(RAW_DIR, "transacciones_raw.json")
    # Orient='records' crea una lista de objetos JSON, típico de APIs
    df_tx.to_json(json_path, orient="records", indent=4)
    print(f"✅ Generado: {json_path}")

if __name__ == "__main__":
    generate_mock_data()
