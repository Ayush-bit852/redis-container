FROM python:3.11-bullseye

# Install basic system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends --fix-missing \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy only the requirements file and install deps first (cache-friendly)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the full application code
COPY . .

# Add non-root user for security
RUN adduser --disabled-password appuser
USER appuser

# Expose the port and run app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]