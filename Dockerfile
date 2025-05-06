# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    netcat-openbsd gcc libpq-dev && \
    apt-get clean

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

ENV DJANGO_SETTINGS_MODULE=fileuploader.settings

# Create required dirs
RUN mkdir -p /app/static /app/media

# Collect static files
RUN python manage.py collectstatic --noinput

# Default command to run Gunicorn server
CMD ["gunicorn", "fileuploader.wsgi:application", "--bind", "0.0.0.0:8000"]
