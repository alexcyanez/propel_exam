# Multi-stage build for Propel ETL Pipeline
FROM python:3.11-slim as base

LABEL maintainer="Alexis Yanez"
LABEL author="Alexis Yanez"
LABEL description="Automated ETL pipeline for Propel data processing and BI integration"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Production stage
FROM base as production

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL script
COPY etl_pipeline.py .

# Create data directories
RUN mkdir -p /data/input /data/output

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import pandas; print('OK')" || exit 1

# Set entry point
ENTRYPOINT ["python", "etl_pipeline.py"]

# Default arguments (can be overridden)
CMD ["/data/input/data propel.xlsx", "/data/output"]
