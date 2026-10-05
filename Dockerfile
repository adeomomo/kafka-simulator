# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Copy the producer script into the container
COPY bess_producer.py .

# Install required Python packages
RUN pip install --no-cache-dir kafka-python

# Run the producer when container starts
CMD ["python", "bess_producer.py"]
