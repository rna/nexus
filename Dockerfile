# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies, including Playwright browsers and their OS dependencies
RUN pip install --no-cache-dir -r requirements.txt 
    && playwright install_deps chromium 
    && playwright install chromium

# Copy the rest of the application's code into the container
COPY . .

# Command to run the scraper when the container launches
CMD ["python", "scraper.py"]
