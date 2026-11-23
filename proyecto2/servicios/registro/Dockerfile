# Servicio interno de registro: solo accesible por la red Docker.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY registro_app/ ./registro_app/

# Puerto interno: no publicar al host desde Docker Compose.
EXPOSE 8003

CMD ["uvicorn", "registro_app.main:app", "--host", "0.0.0.0", "--port", "8003"]