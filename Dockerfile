# Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7999"]


#docker buildx build --platform linux/amd64 \
#   -t omnimap.cr.cloud.ru/omnimap-sync:latest \
#   . --push