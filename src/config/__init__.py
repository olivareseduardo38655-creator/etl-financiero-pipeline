import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT", "5432")

def create_database():
    """Crea la base de datos si no existe."""
    try:
        # Conexión a la base 'postgres' por defecto para crear la nueva DB
        conn = psycopg2.connect(
            user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Verificar si existe
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"✅ Base de datos '{DB_NAME}' creada exitosamente.")
        else:
            print(f"ℹ️ La base de datos '{DB_NAME}' ya existe.")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creando base de datos: {e}")

def create_tables():
    """Crea las tablas con esquema profesional."""
    try:
        conn = psycopg2.connect(
            user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT, dbname=DB_NAME
        )
        cursor = conn.cursor()
        
        # DDL: Definición de tablas
        commands = [
            """
            CREATE TABLE IF NOT EXISTS clientes (
                cliente_id VARCHAR(50) PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE,
                fecha_registro DATE DEFAULT CURRENT_DATE,
                segmento VARCHAR(20)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS productos_financieros (
                producto_id VARCHAR(20) PRIMARY KEY,
                nombre_producto VARCHAR(50) NOT NULL,
                tipo VARCHAR(20) CHECK (tipo IN ('CREDITO', 'DEBITO', 'INVERSION'))
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS transacciones (
                transaccion_id VARCHAR(50) PRIMARY KEY,
                cliente_id VARCHAR(50) REFERENCES clientes(cliente_id),
                producto_id VARCHAR(20) REFERENCES productos_financieros(producto_id),
                monto DECIMAL(15, 2) NOT NULL CHECK (monto != 0),
                fecha_transaccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tipo_movimiento VARCHAR(10) CHECK (tipo_movimiento IN ('ENTRADA', 'SALIDA'))
            )
            """
        ]
        
        for command in commands:
            cursor.execute(command)
        
        conn.commit()
        print("✅ Tablas creadas/verificadas correctamente (Esquema Snowflake).")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando configuración de BD...")
    create_database()
    create_tables()
    print("🏁 Configuración finalizada.")
