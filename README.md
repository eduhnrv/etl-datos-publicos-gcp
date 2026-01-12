# ETL Datos Públicos – Ministerio de Salud de Chile

#### Autor: Eduardo S. Henríquez N.
#### Fecha : 11 / 01 / 26.  


## Descripción general

Este proyecto implementa un pipeline ETL completo orientado a un escenario real de Data Engineering, utilizando datos públicos del Ministerio de Salud de Chile (MINSAL). El objetivo principal es demostrar el diseño, validación, ejecución y despliegue de un flujo de datos robusto, testeado y reproducible, desde archivos crudos hasta su disponibilidad en Google Cloud Storage.

El foco del proyecto está puesto en la calidad del dato, la separación clara de responsabilidades, el manejo explícito de errores y las buenas prácticas de ingeniería, más que en el análisis estadístico de la información.

---

## Datos de origen

El pipeline procesa un dataset público del Ministerio de Salud de Chile, relacionados con estadísticas epidemiológicas de defunciones a lo largo del país, contemplando todas las regiones del territorio.

Características principales de los datos:

- **Fuente**: Ministerio de Salud de Chile (MINSAL), portal de datos abiertos.
- **Formato original**: CSV delimitado por `|`.
- **Periodo de referencia**: desde el año **2010 en adelante**.
- **Frecuencia de actualización**: **semanal**.
- **Granularidad**: Una observación semanal de defunciones por región, sexo y grupo etario, para un año estadístico determinado.
- **Volumen aproximado**: ~133.500 registros en dataset.

Los archivos originales presentan un esquema fijo pero validaciones mínimas, lo que justifica la incorporación de controles tempranos de estructura, consistencia y valores faltantes durante la etapa de extracción.

---

## Arquitectura del pipeline

El flujo ETL se compone de las siguientes etapas:

1. **Extract**  
   - Validación de existencia del archivo.
   - Lectura controlada del CSV.
   - Verificación de esquema esperado.
   - Manejo explícito de errores comunes (archivo inexistente, vacío, esquema inválido, errores de parsing).

2. **Inspect (calidad de datos)**  
   - Revisión de valores nulos.
   - Validación básica de integridad del DataFrame.

3. **Transform**  
   - Limpieza y estandarización de columnas.
   - Transformaciones orientadas a normalizar rangos etarios y enriquecer el dataset.
   - Generación de un dataset listo para consumo analítico.

4. **Load (local)**  
   - Persistencia del dataset transformado en el filesystem local.
   - Creación automática de directorios si no existen.

5. **Load (Google Cloud Storage)**  
   - Validación del archivo transformado.
   - Carga del dataset versionado por fecha de ejecución en un bucket de GCS.

<br><br><br>
---

## Estructura del proyecto

```text

etl-datos-publicos-gcp/
├── src/
│   ├── extract.py
│   ├── inspect_csv.py
│   ├── transform.py
│   ├── load.py
│   ├── load_gcs.py
│   ├── logger.py
│   └── main.py
│
├── tests/
│   ├── test_extract.py
│   ├── test_inspect_csv.py
│   ├── test_transform.py
│   ├── test_load.py
│   ├── test_load_gcs.py
│   └── test_main.py
│
├── data/
│   ├── raw/
│   │   └── dataset_raw.csv
│   └── transformed/
│
├── docs/
│   └── data_source.md
│
├── Dockerfile
├── Dockerfile.test
├── requirements.txt
├── requirements-test.txt
└── README.md
```
---

## Testing y calidad

El proyecto cuenta con una **batería completa de tests unitarios**, cubriendo:

- Casos exitosos.
- Errores esperados por datos inválidos.
- Manejo de excepciones.
- Comportamiento del pipeline completo.

Resultados actuales:

- **24 tests unitarios** ejecutados con `pytest`.
- **Cobertura total ~93 %**.
- Todas las etapas del pipeline validadas de forma aislada.

---

## Ejecución con Docker

El pipeline está completamente containerizado.

### Pipeline.
### Construcción de la imagen

```bash
sudo docker build -t etl-datos-publicos -f Dockerfile .
```
<br>
### Ejecución del pipeline

```bash
sudo docker run --rm \
  -v $PWD/data:/app/data \
  -v $PWD/cred.json:/app/cred.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/cred.json \
  etl-datos-publicos
```
<br>

### Testing
### Construcción de la imagen
```bash
sudo docker build -t etl-test -f Dockerfile.test .
```

### Ejecución de pruebas
```bash
sudo docker run --rm etl-test
```

---

## Objetivo profesional del proyecto

Este proyecto fue diseñado explícitamente como **pieza de portafolio profesional** para roles de:

- **Data Engineer**
- **Analytics Engineer**

Demuestra experiencia práctica en:

- Diseño de pipelines ETL reales.
- Validación y control de calidad de datos.
- Testing automatizado.
- Containerización con Docker.
- Integración con servicios cloud (Google Cloud Storage).

No busca realizar análisis estadístico ni modelado predictivo, sino mostrar criterio sobre ingeniería y buenas prácticas aplicadas a datos reales.

---

## Estado del proyecto

- Pipeline funcional end-to-end  
- Tests alcanzando valor cercano al 100 %  
- Cobertura alta y controlada  
- Listo para extensión (BigQuery, orquestación, CI/CD)

---

