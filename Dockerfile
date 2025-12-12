FROM python:3.10.12-slim

# Install Java and required utilities (ONLY OpenJDK)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        wget curl ca-certificates procps tini \
        libcurl4-openssl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Correct JAVA_HOME for Debian OpenJDK
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# PySpark environment
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

# Verify Java
RUN java -version

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

USER root

CMD ["python", "src/main.py"]