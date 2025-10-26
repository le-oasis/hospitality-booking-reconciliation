FROM apache/airflow:2.7.3-python3.11

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install additional packages as airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to root for entrypoint (airflow will switch to airflow user)
USER root