FROM python:3.11-slim

WORKDIR /src

COPY app /src/app
COPY config /src/config
COPY main.py /src/

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

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
    && rm -rf /var/lib/apt/lists/* \
    && pip install  -r /requirements.txt

RUN pwd && ls

# Add the SQL tools to PATH
ENV PATH="/opt/mssql-tools18/bin:${PATH}"


CMD ["python", "main.py"]
