FROM apache/airflow:3.1.0

# Switch to root to install system packages
USER root

# Install PostgreSQL dev tools and compiler
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        python3-dev \
        build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install Python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
