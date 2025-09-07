# Base image
FROM apache/airflow:3.0.6

ENV AIRFLOW_HOME=/opt/airflow

# Switch to the airflow user
USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Copy the requirements file into the container and package dependencies
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt


# Add dbt to PATH
ENV PATH="/root/.local/bin:${PATH}"

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

COPY scripts scripts

USER $AIRFLOW_UID