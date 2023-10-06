# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8.15-slim-buster

# Warning: A port below 1024 has been exposed. This requires the image to run as a root user which is not a best practice.
# For more information, please refer to https://aka.ms/vscode-docker-python-user-rights`
# EXPOSE 80
EXPOSE 8080

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# build variables.
ENV DEBIAN_FRONTEND noninteractive

# install Microsoft SQL Server requirements.
# install postgres ODBC driver 
ENV ACCEPT_EULA=Y
RUN apt-get update -y && apt-get update \
  && apt-get install -y --no-install-recommends curl gcc g++ gnupg unixodbc unixodbc-dev odbc-postgresql

# Add SQL Server ODBC Driver 17 for Ubuntu 18.04
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends --allow-unauthenticated msodbcsql17 mssql-tools \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc


# Set the working directory to /src
WORKDIR /src

# Copy the Python project files (excluding .venv, .git, etc.) into the container
COPY . .

# Install Poetry
RUN pip install poetry

# Install project dependencies using Poetry
RUN poetry config virtualenvs.create false && \
  poetry install

# Creates a non-root user with an explicit UID and adds permission to access the /src folder
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /src

# Switch to the non-root user
USER appuser

# During debugging, this entry point will be overridden.
CMD ["gunicorn", "--workers", "3", "--bind", ":8080", "app:server", "--timeout", "90"]
