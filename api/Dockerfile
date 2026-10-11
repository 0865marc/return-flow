FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /api

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cache the healthcheck's stdlib imports once, instead of compiling them on
# every probe while sharing the API's CPU quota.
RUN env -u PYTHONDONTWRITEBYTECODE python -c "import urllib.request"

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
