# Use a lightweight Python image
FROM python:3.9-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scraper script into the container
COPY distributed_scraper.py .

# Setup the entrypoint (we will define arguments in docker-compose)
ENTRYPOINT ["python", "distributed_scraper.py"]