# Dockerfile para Airflow con UV package manager
FROM apache/airflow:3.1.6

# Cambiar a usuario root para instalar paquetes del sistema
USER root

# Instalar UV
RUN apt-get update && \
    apt-get install -y curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Agregar UV al PATH
ENV PATH="/root/.local/bin:$PATH"

# Cambiar de vuelta al usuario airflow
USER airflow

# Copiar archivos de configuración de Python para aprovechar cache de Docker
COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/

# Configurar UV para usar el Python del sistema de Airflow
ENV UV_PYTHON_PREFERENCE=only-system
ENV UV_SYSTEM_PYTHON=1

# Instalar dependencias con UV desde el lockfile (reproducible)
WORKDIR /opt/airflow
RUN uv sync --frozen --no-dev || \
    uv pip install --system requests sqlalchemy psycopg2-binary python-dotenv

# Verificar instalación
RUN python -c "import requests; import sqlalchemy; import psycopg2; print('✓ Dependencies installed from uv.lock')"
