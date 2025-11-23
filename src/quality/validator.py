import pandas as pd
import numpy as np
import os

class DataValidator:
    """
    Firewall de calidad de datos.
    Separa los datos en 'VALID' (pasan a DB) e 'INVALID' (se van a cuarentena).
    Implementa reglas de negocio financieras.
    """
    
    def __init__(self):
        # Rutas para guardar reportes de rechazo
        # Usamos una ruta absoluta segura basada en la ubicación de este archivo
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.error_dir = os.path.join(self.base_dir, "data", "error")

    def _save_quarantine(self, df_error: pd.DataFrame, filename: str):
        """Guarda los datos rechazados en formato CSV para auditoría."""
        if not df_error.empty:
            path = os.path.join(self.error_dir, filename)
            df_error.to_csv(path, index=False)
            print(f"   ⚠️  ALERTA: {len(df_error)} registros enviados a Cuarentena -> {path}")

    def validate_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reglas:
        1. ID no puede ser nulo.
        2. Email debe contener '@'.
        3. No duplicados en ID.
        4. Nombre no puede ser nulo (NUEVA REGLA).
        """
        print("   🛡️  Validando Clientes...")
        
        df_valid = df.copy()
        
        # REGLA 1: Integridad de ID
        mask_id_null = df_valid['cliente_id'].isnull() | (df_valid['cliente_id'] == '')
        
        # REGLA 2: Formato Email
        mask_bad_email = ~df_valid['email'].astype(str).str.contains('@', na=False)
        
        # REGLA 3: Duplicados
        mask_duplicated = df_valid.duplicated(subset=['cliente_id'], keep='first')

        # REGLA 4: Nombre Obligatorio (LA QUE FALTABA)
        mask_name_null = df_valid['nombre'].isnull() | (df_valid['nombre'] == '')
        
        # Combinar fallos
        mask_fail = mask_id_null | mask_bad_email | mask_duplicated | mask_name_null
        
        # Separar
        df_rejected = df_valid[mask_fail].copy()
        df_rejected['error_reason'] = 'Invalid ID, Email, Name or Duplicate'
        
        df_approved = df_valid[~mask_fail]
        
        # Reportar
        self._save_quarantine(df_rejected, "clientes_rejected.csv")
        print(f"   ✔ Aprobados: {len(df_approved)} | ❌ Rechazados: {len(df_rejected)}")
        
        return df_approved

    def validate_transacciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reglas:
        1. Monto no puede ser 0 o negativo.
        """
        print("   🛡️  Validando Transacciones...")
        df_valid = df.copy()
        
        # REGLA 1: Monto Positivo
        mask_invalid_amount = df_valid['monto'] <= 0
        
        # Separar
        df_rejected = df_valid[mask_invalid_amount].copy()
        df_rejected['error_reason'] = 'Monto invalido (<= 0)'
        
        df_approved = df_valid[~mask_invalid_amount]
        
        # Reportar
        self._save_quarantine(df_rejected, "transacciones_rejected.csv")
        print(f"   ✔ Aprobados: {len(df_approved)} | ❌ Rechazados: {len(df_rejected)}")
        
        return df_approved
