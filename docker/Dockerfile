FROM python:3.13-slim

WORKDIR /app

# Install system dependencies required for Python packages and Java for Spark
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    curl \
    build-essential \
    && ln -s /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/default-java \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements file
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN python3 -m pip install --no-cache-dir -r /app/requirements.txt

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
