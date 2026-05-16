FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Non-root user for security
RUN useradd -m -u 1000 pipeline && chown -R pipeline:pipeline /app
USER pipeline

EXPOSE 8000

CMD ["uvicorn", "api.inbound_api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
