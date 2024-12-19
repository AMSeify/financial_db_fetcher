# Start from a lightweight Python base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . /app/

# Set environment variables if needed
# ENV SOME_ENV_VAR=some_value

# Run the main async tasks
CMD ["python", "main.py"]
