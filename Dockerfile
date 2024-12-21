# Start from a lightweight Python base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Install system dependencies and Microsoft ODBC driver
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    unixodbc \
    gnupg2 \
    libgssapi-krb5-2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add the SQL tools to PATH
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Copy the rest of the application code
COPY . /app/

# Set environment variables if needed
# ENV SOME_ENV_VAR=some_value

# Run the main async tasks
CMD ["python", "main.py"]
