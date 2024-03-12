# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11-slim-buster

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

# Copy the Python project files (excluding .venv, .git, etc.) into the container
COPY . . 
# Install Poetry
RUN pip install poetry
# RUN pip install gunicorn

# Install project dependencies using Poetry in root directory
RUN poetry config virtualenvs.create false && \
  poetry install

# Set the working directory to /src
WORKDIR /src

# Creates a non-root user with an explicit UID and adds permission to access the /src folder
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /src

# Switch to the non-root user
USER appuser

# During debugging, this entry point will be overridden.
CMD ["gunicorn", "--workers", "2", "--bind", ":8080", "app:server", "--timeout", "90"]
