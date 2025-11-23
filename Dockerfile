# 1. Usamos una imagen base de Python profesional (ligera y segura)
FROM python:3.10-slim

# 2. Evitamos que Python genere archivos caché (.pyc) y buffer de salida
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. Instalar dependencias del sistema necesarias para PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 5. Copiar dependencias e instalarlas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copiar todo el código del proyecto
COPY . .

# 7. Exponer el puerto de Streamlit (Dashboard)
EXPOSE 8501

# 8. Comando por defecto: Ejecutar el Dashboard
CMD ["streamlit", "run", "src/dashboard.py", "--server.address=0.0.0.0"]
