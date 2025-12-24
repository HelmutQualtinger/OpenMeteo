# Use a slim Python Alpine image
FROM python:3.13-alpine

# Set the working directory in the container
WORKDIR /app

# Copy dependency definition
COPY pyproject.toml ./

# Install dependencies using pip from pyproject.toml
RUN pip install --no-cache-dir .

# Copy the rest of the application files
COPY . .

# Command to run the scheduler
CMD ["python3", "-u", "scheduler.py"]