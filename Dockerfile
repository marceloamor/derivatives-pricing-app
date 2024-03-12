# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.11-slim AS builder

# build variables.
ENV DEBIAN_FRONTEND noninteractive

RUN python -m pip install poetry 

WORKDIR /app/build

COPY pyproject.toml .
COPY poetry.lock .
COPY README.md .
COPY dependencies/ /app/build/dependencies/
COPY src/ /app/build/frontend/

RUN poetry build --format wheel


FROM python:3.11-slim

COPY dependencies/ /app/build/dependencies/

RUN --mount=type=bind,from=builder,source=/app/build/dist,target=/app/wheels \
  pip install --no-cache-dir /app/wheels/frontend*

# EXPOSE 80
EXPOSE 8080
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
RUN adduser -u 4738 --disabled-password --gecos "" appuser && \
  chown -R appuser /app 
USER appuser

# Switch to the non-root user
USER appuser

CMD ["gunicorn", "--workers", "3", "--bind", ":8080", "app:server", "--timeout", "90"]
