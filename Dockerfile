# Builder stage
FROM python:3.11.9-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY requirements.txt constraints.txt ./

# Install dependencies with pip
RUN pip install --no-cache-dir -r requirements.txt


# Runtime stage
FROM python:3.11.9-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    make \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code and configs
COPY . .

# Install package (registers entry points and makes `from src...` robust)
RUN python -m pip install --no-cache-dir -e .

# Create non-root user and fix ownership
RUN useradd -m dovah && chown -R dovah:dovah /app
USER dovah

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    DOVAH_TENANT_SALT=dev_only_do_not_use

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "from src.ingest.hdfs_loader import HDFSLoader; HDFSLoader()"

# Default command
CMD ["make", "phase3-all", "PHASE=phase3", "PY=python", "SH=sh -c"]
