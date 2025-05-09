# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    netcat-openbsd gcc libpq-dev && \
    apt-get clean

# Copy requirements.txt first to leverage Docker caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files (this includes server.py and other relevant files)
COPY . .

# Expose port 8000 for FastAPI
EXPOSE 8000

# Set environment variables (optional if needed for your application)
# ENV REDIS_HOST=localhost
# ENV REDIS_PORT=6379
# ENV REDIS_DB=0
# ENV LOG_LEVEL=INFO

# Default command to run FastAPI server
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
