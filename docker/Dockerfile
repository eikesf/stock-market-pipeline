FROM python:3.13-slim AS base

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
ENV PATH="/app/.venv/bin:$JAVA_HOME/bin:$PATH"

# Install uv
RUN pip install uv==0.11.19

# Copy dependency files
COPY pyproject.toml uv.lock /app/

# Builder - install all deps including dev (for CI/testing)
FROM base as builder

RUN uv sync --frozen --group dev --no-install-project

# Runtime - lean production image, no test/lint tooling
FROM base as runtime

RUN uv sync --frozen --no-dev --no-install-project

# Copy source code
COPY src/ /app/src/

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
