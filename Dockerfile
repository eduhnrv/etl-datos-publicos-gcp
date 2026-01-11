#imagen base ligera de python
FROM python:3.13-slim

#Directorio de trabajo dentro del docker
WORKDIR /app

#Copia Dependencias
COPY requirements.txt .

#copia el proyecto
COPY . /app

#Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

#Evitar buffering en logs
ENV PYTHONUNBUFFERED=1

#comando para ejecución de pipeline
CMD ["python", "-m", "src.main"]


