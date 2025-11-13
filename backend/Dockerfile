FROM python:3.11-slim

WORKDIR /app

# Install system dependencies in one layer and clean up immediately
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    rustc \
    cargo \
    pkg-config \
    libssl-dev \
    libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/uploads /app/vector_store /app/faiss_index

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
