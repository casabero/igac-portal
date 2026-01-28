# Usamos Python 3.11 Slim (Ahorro de espacio en tu SSD)
FROM python:3.11-slim

# Variables de entorno para optimización
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias de sistema para Matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfreetype6 \
    libpng16-16 \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código fuente
COPY . .

# Crear la carpeta data para asegurar que existe (Persistencia Logs)
RUN mkdir -p /app/data

# Exponer puerto 8000 (Estándar Coolify)
EXPOSE 8000

# Comando de arranque con Gunicorn (2 workers para tu CPU Ryzen)
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "120", "app:app"]