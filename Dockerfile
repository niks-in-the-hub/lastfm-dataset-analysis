FROM python:3.10-slim

# Install utilities required by Spark
RUN apt-get update && \
    apt-get install -y wget curl gnupg ca-certificates procps && \
    apt-get clean

# Add Adoptium (Temurin) key and repo
RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public \
    | gpg --dearmor \
    | tee /usr/share/keyrings/adoptium.gpg > /dev/null

RUN echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] \
    https://packages.adoptium.net/artifactory/deb \
    $(. /etc/os-release && echo $VERSION_CODENAME) main" \
    > /etc/apt/sources.list.d/adoptium.list

# Install Temurin JDK 17 (ARM64 on Apple Silicon)
RUN apt-get update && apt-get install -y temurin-17-jdk && apt-get clean

# Set JAVA_HOME based on actual installed directory
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Verify Java installs correctly
RUN java -version

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY src /app/src

WORKDIR /app/src

# Default command: run pipeline
CMD ["python", "main.py"]