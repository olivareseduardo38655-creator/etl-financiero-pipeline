# Pipeline de Ingeniería de Datos Financieros

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)

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

El flujo de datos sigue una arquitectura modular lineal controlada por eventos:

```mermaid
graph LR
    A[Fuentes de Datos] -->|Extract| B(Extractor)
    B -->|Raw Data| C{Transformador}
    C -->|Clean Data| D[Validador DQ]
    D -->|Aprobados| E[(PostgreSQL DW)]
    D -->|Rechazados| F[Carpeta Cuarentena]
    E -->|SQL Query| G[Streamlit Dashboard]
