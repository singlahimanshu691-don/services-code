FROM python:3.11-slim

WORKDIR /app

# Install Docker CLI so the dashboard can run `docker logs` and `docker inspect`
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY dashboard.html .
COPY ./images ./images

EXPOSE 8090

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090", "--log-level", "warning"]
