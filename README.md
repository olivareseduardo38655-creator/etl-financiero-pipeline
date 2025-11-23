# 🏦 Financial Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success)

## 📖 Descripción del Proyecto

Este proyecto implementa un **Pipeline ETL (Extract, Transform, Load) End-to-End** diseñado para el sector financiero. Simula la ingesta de datos transaccionales, aplicando reglas estrictas de **Calidad de Datos (Data Quality)** y **Gobierno de Datos** antes de persistir la información en un Data Warehouse (PostgreSQL).

Incluye un módulo de visualización científica (Dashboard) para la toma de decisiones basada en datos.

### 🎯 Objetivos Técnicos
1.  **Ingesta Agnóstica:** Capacidad de procesar múltiples formatos (CSV, JSON, Excel) de forma unificada.
2.  **Data Quality Firewall:** Sistema de validación que intercepta y aísla registros corruptos o fraudulentos (Cuarentena).
3.  **Integridad Referencial:** Lógica de carga que asegura consistencia entre Clientes, Productos y Transacciones ("Source of Truth validation").
4.  **Visibilidad:** Dashboard interactivo para auditoría y análisis de negocio.

---

## 🏗️ Arquitectura del Sistema

El flujo de datos sigue una arquitectura modular:

```mermaid
graph LR
    A[Fuentes de Datos] -->|Extract| B(Extractor)
    B -->|Raw Data| C{Transformador}
    C -->|Clean Data| D[Validador DQ]
    D -->|Aprobados| E[(PostgreSQL DW)]
    D -->|Rechazados| F[Carpeta Cuarentena]
    E -->|SQL Query| G[Streamlit Dashboard]
