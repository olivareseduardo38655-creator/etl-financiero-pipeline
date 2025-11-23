# Pipeline de Ingeniería de Datos Financieros

![Python](https://img.shields.io/badge/Python-3.10-555555?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-555555?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Compose-555555?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production_Ready-555555?style=flat-square)

## Descripción del Proyecto

Este proyecto implementa un Pipeline ETL (Extract, Transform, Load) "End-to-End" diseñado específicamente para el sector financiero. El sistema simula la ingesta de datos transaccionales brutos, aplicando reglas estrictas de Calidad de Datos (Data Quality) y Gobierno de Datos antes de persistir la información depurada en un Data Warehouse basado en PostgreSQL.

Adicionalmente, incluye un módulo de visualización científica (Dashboard) desarrollado en Streamlit para facilitar la auditoría de datos y la toma de decisiones estratégicas.

## Objetivos Técnicos

1.  **Ingesta Agnóstica:** Capacidad unificada para procesar múltiples formatos de origen (CSV, JSON, Excel).
2.  **Firewall de Calidad de Datos:** Sistema de validación que intercepta, aísla y reporta registros corruptos o fraudulentos en una zona de cuarentena.
3.  **Integridad Referencial Estricta:** Lógica de carga que asegura la consistencia matemática entre Clientes, Productos y Transacciones (Validación contra "Source of Truth").
4.  **Infraestructura como Código:** Despliegue contenerizado mediante Docker para garantizar la reproducibilidad del entorno.

---

## Arquitectura del Sistema

El flujo de datos sigue una arquitectura modular lineal:

* **Extract:** Módulo de fábrica para la ingestión de múltiples fuentes.
* **Transform:** Limpieza, normalización de tipos y estandarización de formatos (Pandas).
* **Quality:** Validación de reglas de negocio y lógica de segregación (Cuarentena).
* **Load:** Carga transaccional segura hacia PostgreSQL utilizando SQLAlchemy.
* **Visualization:** Reporte analítico interactivo.

## Estructura del Repositorio

* `data/`: Almacenamiento temporal de datos (Raw y Error/Cuarentena).
* `logs/`: Historial de ejecución y trazas de auditoría.
* `src/`: Código fuente del proyecto.
    * `config/`: Configuración de base de datos y variables de entorno.
    * `extract/`: Scripts de extracción.
    * `transform/`: Lógica de limpieza y transformación.
    * `quality/`: Validadores y reglas de negocio.
    * `load/`: Carga y conexión a base de datos.
* `requirements.txt`: Lista de dependencias del proyecto.
* `Dockerfile`: Configuración para la contenerización del sistema.
* `docker-compose.yml`: Orquestación de servicios (App + Base de Datos).

---

## Guía de Instalación y Ejecución

Este proyecto puede ejecutarse localmente o mediante contenedores Docker (recomendado).

### Opción A: Ejecución con Docker (Recomendado)

Esta opción despliega automáticamente la base de datos y el pipeline sin necesidad de configuraciones locales.

1.  **Construir y levantar servicios:**
    ```bash
    docker-compose up --build
    ```

2.  **Ejecutar Pipeline ETL (en una nueva terminal):**
    ```bash
    # Inicializar Base de Datos
    docker-compose run --rm etl-app python src/config/init_db.py
    
    # Generar Datos de Prueba
    docker-compose run --rm etl-app python src/utils/mock_data_generator.py
    
    # Ejecutar Proceso ETL Maestro
    docker-compose run --rm etl-app python src/run_pipeline.py
    ```

3.  **Acceder al Dashboard:**
    Visitar: `http://localhost:8501`

### Opción B: Ejecución Local Manual

1.  **Configuración de Entorno:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

2.  **Variables de Entorno:**
    Crear un archivo `.env` en la raíz con las credenciales de PostgreSQL local:
    ```ini
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=financiero_db
    DB_USER=postgres
    DB_PASSWORD=su_password
    ```

3.  **Ejecución de Scripts:**
    ```bash
    python src/config/init_db.py
    python src/utils/mock_data_generator.py
    python src/run_pipeline.py
    streamlit run src/dashboard.py
    ```

---

## Reglas de Gobierno de Datos

El sistema aplica las siguientes reglas de negocio para garantizar la integridad de la información:

| Entidad | Regla de Validación | Acción en caso de fallo |
| :--- | :--- | :--- |
| **Clientes** | Email con formato válido y Nombre no nulo | Rechazo (Cuarentena) |
| **Transacciones** | Monto positivo (>0) | Rechazo (Cuarentena) |
| **Integridad** | Cliente y Producto deben existir en DB | Descarte silencioso (Log Warning) |

---

## Autor

Proyecto desarrollado como parte de un portafolio profesional de **Ingeniería Financiera y Ciencia de Datos**.
