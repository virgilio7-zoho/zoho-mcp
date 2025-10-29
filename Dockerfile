
# Imagen ligera
FROM python:3.11-slim

# No crear .pyc y forzar salida flushing
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Paquetes del sistema mínimos
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código
COPY app ./app

# Puerto
ENV PORT=8000
EXPOSE 8000

# Comando
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
