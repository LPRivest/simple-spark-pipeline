# Base image that already includes Java 17
FROM openjdk:17-slim

# Install Python and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Python as default
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PySpark and helpers
RUN pip install --no-cache-dir pyspark==3.5.1 pandas pytest pyyaml

# Working directory
WORKDIR /app

ENV PYTHONPATH="/app"

# Default command
CMD ["python"]