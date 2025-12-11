FROM python:3.10.12-slim

# Install Java and required utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget curl gnupg ca-certificates procps tini && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public \
        | gpg --dearmor > /usr/share/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] \
        https://packages.adoptium.net/artifactory/deb \
        $(. /etc/os-release && echo $VERSION_CODENAME) main" \
        > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends temurin-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set stable JAVA_HOME (correct for Temurin installs)
ENV JAVA_HOME="/usr/lib/jvm/temurin-17-jdk"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN java -version

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Remain root user (for guaranteed write permissions on mounted volumes)
USER root

CMD ["python", "src/main.py"]

