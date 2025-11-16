FROM apache/airflow:2.8.0-python3.11

USER root

# Install OS dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies (Airflow already installed)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
