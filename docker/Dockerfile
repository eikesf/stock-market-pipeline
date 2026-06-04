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

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock /app/

# Install dependencies
RUN uv pip install --system --no-cache -r /app/pyproject.toml --group dev

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
