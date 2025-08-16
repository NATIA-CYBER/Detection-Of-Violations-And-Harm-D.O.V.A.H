# Builder stage
FROM python:3.11.9-slim@sha256:c0c31a94e3c3bb3d811b2e6951a8eb6c0a3ca5e9c2e0a97b6c3c4c98f0e3324 as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY requirements.txt constraints.txt ./

# Install dependencies with pip
RUN pip install --no-cache-dir -r requirements.txt -c constraints.txt

# Runtime stage
FROM python:3.11.9-slim@sha256:c0c31a94e3c3bb3d811b2e6951a8eb6c0a3ca5e9c2e0a97b6c3c4c98f0e3324

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code and configs
COPY . .

# Create non-root user
RUN useradd -m dovah && \
    chown -R dovah:dovah /app
USER dovah

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    DOVAH_TENANT_SALT=dev_only_do_not_use

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "from src.ingest.hdfs_loader import HDFSLoader; HDFSLoader()"

# Default command
CMD ["make", "verify_day2"]
