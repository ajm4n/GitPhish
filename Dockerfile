FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir \
    flask==2.3.3 \
    flask-cors==4.0.0 \
    boto3==1.28.0 \
    botocore==1.31.0 \
    twilio==8.5.0 \
    schedule==1.2.0 \
    sqlalchemy \
    requests \
    cryptography \
    pyjwt \
    python-dotenv \
    urllib3==1.26.18 \
    jmespath==1.0.1 \
    python-dateutil==2.8.2 \
    pygithub \
    gitpython

# Set Python path
ENV PYTHONPATH=/app

# Create data directory for SQLite database
RUN mkdir -p /app/data

# Expose port
EXPOSE 8080

# Start the server
CMD ["python", "gitphish/core/gui/server.py"]