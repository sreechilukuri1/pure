# Multi-stage build for Python 3.11
FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Default job selector (can be overridden at runtime via env or CLI)
ENV JOB_TYPE=""

# Image metadata
LABEL maintainer="your-team@example.com"

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for building with a home directory (-m)
RUN groupadd -r buildgroup && useradd -r -m -g buildgroup builduser

# Install Python dependencies as non-root
WORKDIR /app
COPY requirements.txt .
RUN chown builduser:buildgroup requirements.txt

USER builduser
RUN pip install --user --no-cache-dir --no-warn-script-location \
    -r requirements.txt

# Production stage
FROM python:3.11-slim

# Install only essential runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for runtime with a home directory (-m)
RUN groupadd -r appgroup && useradd -r -m -g appgroup -s /bin/bash appuser

# Copy Python packages from builder
COPY --from=builder /home/builduser/.local /home/appuser/.local

# Set working directory
WORKDIR /app

# Copy application code (all files from src directory)
COPY --chown=appuser:appgroup src/ ./

# Set environment variables
# Path includes the local bin and the specific site-packages directory
ENV PATH="/home/appuser/.local/bin:$PATH" \
    PYTHONPATH="/app:/home/appuser/.local/lib/python3.11/site-packages" \
    PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Entrypoint for the Pure Data Pipeline
CMD ["python", "entrypoint.py"]