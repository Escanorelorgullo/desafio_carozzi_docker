# Imagen base con Python 3.11 minimal
FROM python:3.11-slim

# Evita preguntas interactivas en apt
ENV DEBIAN_FRONTEND=noninteractive

# Instalamos Java 21 para que PySpark funcione
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre && \
    rm -rf /var/lib/apt/lists/*

# Variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copiamos solo requirements primero (para cachear instalaciones)
COPY requirements.txt .

# Instalamos dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# El código y data se montan como volumen con docker-compose,
# por eso NO los copiamos aquí.

# Comando por defecto
CMD ["python", "src/main.py"]
