import pandas as pd
import numpy as np

class DataTransformer:
    """
    Módulo encargado de la limpieza, normalización y estandarización de datos.
    Convierte datos 'RAW' (crudos) en datos 'SILVER' (limpios estructuralmente).
    """
    
    def _clean_id_column(self, series: pd.Series) -> pd.Series:
        """Helper para limpiar columnas de ID (quita espacios y mayúsculas)."""
        return series.astype(str).str.strip().str.upper()

    def clean_clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza datos de clientes."""
        print("   🔄 Transformando Clientes...")
        df = df.copy()
        
        # 0. Limpieza CRÍTICA de IDs (para evitar errores de SQL)
        if 'cliente_id' in df.columns:
            df['cliente_id'] = self._clean_id_column(df['cliente_id'])

        # 1. Estandarizar Email
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower().str.strip()
            
        # 2. Estandarizar Segmento
        if 'segmento' in df.columns:
            df['segmento'] = df['segmento'].str.upper().str.strip()
            
        # 3. Convertir fechas
        if 'fecha_registro' in df.columns:
            df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce')
            
        return df

    def clean_productos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza catálogo de productos."""
        print("   🔄 Transformando Productos...")
        df = df.copy()
        
        # 0. Limpieza CRÍTICA de IDs
        if 'producto_id' in df.columns:
            df['producto_id'] = self._clean_id_column(df['producto_id'])
        
        # 1. Nombres de producto
        if 'nombre_producto' in df.columns:
            df['nombre_producto'] = df['nombre_producto'].str.title().str.strip()
            
        # 2. Tipo de producto
        if 'tipo' in df.columns:
            df['tipo'] = df['tipo'].str.upper().str.strip()
            
        return df

    def clean_transacciones(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza transacciones financieras."""
        print("   🔄 Transformando Transacciones...")
        df = df.copy()
        
        # 0. Limpieza CRÍTICA de IDs (Foreign Keys)
        if 'transaccion_id' in df.columns:
            df['transaccion_id'] = self._clean_id_column(df['transaccion_id'])
        if 'cliente_id' in df.columns:
            df['cliente_id'] = self._clean_id_column(df['cliente_id'])
        if 'producto_id' in df.columns:
            df['producto_id'] = self._clean_id_column(df['producto_id'])
        
        # 1. Convertir fechas
        if 'fecha_transaccion' in df.columns:
            df['fecha_transaccion'] = pd.to_datetime(df['fecha_transaccion'], errors='coerce')
            
        # 2. Asegurar monto float
        if 'monto' in df.columns:
            df['monto'] = pd.to_numeric(df['monto'], errors='coerce')
            
        # 3. Tipo de movimiento
        if 'tipo_movimiento' in df.columns:
            df['tipo_movimiento'] = df['tipo_movimiento'].str.upper().str.strip()
            
        return df
