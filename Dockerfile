# Use the official Python image from Docker Hub
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Install any dependencies from requirements.txt if it exists
RUN pip install --no-cache-dir -r requirements.txt || true

# Create directory for persistent data
RUN mkdir -p /data

# Set Environment Variables for Persistence
ENV DB_PATH=/data/data_antrol.db

# Command to run the application using dynamic PORT for PaaS
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT:-3000} --reload"