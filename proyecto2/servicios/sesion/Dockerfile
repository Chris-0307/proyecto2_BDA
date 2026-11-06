# Se utiliza Python 3.12 con una imagen ligera.
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor.
WORKDIR /app

# Se copian las dependencias del servicio.
COPY requirements.txt .

# Se instalan las bibliotecas necesarias.
RUN pip install --no-cache-dir -r requirements.txt

# Se copia el codigo de la aplicacion.
COPY app/ ./app/

# Puerto interno del servicio de sesion.
# No se publica directamente al host.
EXPOSE 8001

# Se inicia la API REST mediante Uvicorn.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
