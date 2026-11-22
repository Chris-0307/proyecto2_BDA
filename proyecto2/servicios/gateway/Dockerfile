# Gateway: único servicio publicado al host (puerto 8000).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway_app/ ./gateway_app/

EXPOSE 8000

CMD ["uvicorn", "gateway_app.main:app", "--host", "0.0.0.0", "--port", "8000"]
