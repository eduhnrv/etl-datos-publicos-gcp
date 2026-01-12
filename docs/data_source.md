# Fuente de datos
#### Autor: Eduardo S. Henríquez N.
#### Fecha : 11 / 01 / 26. 
#### Proyecto : ETL Datos Públicos - Ministerio de Salud de Chile. 


## Descripción general

Este proyecto utiliza un único dataset público en estado raw, proveniente del Ministerio de Salud de Chile (MINSAL), el cual es procesado a lo largo de distintas etapas del pipeline ETL (raw → transformed).

El objetivo de este documento es describir con precisión el origen, las características y las limitaciones conocidas del dataset utilizado, proporcionando contexto técnico y trazabilidad sobre la fuente de datos.

---

## Organismo proveedor

- **Institución**: Ministerio de Salud de Chile (MINSAL)
- **Dependencia técnica**: Departamento de Estadísticas e Información de Salud (DEIS)
- **Tipo de datos**: Datos públicos oficiales

Los datos son publicados por el MINSAL a través de sus canales de datos abiertos y son utilizados habitualmente para análisis epidemiológicos, reportes oficiales y estudios de salud pública.

---

## Dataset utilizado

- **Nombre del archivo**: `def_semana_epidemiologica.csv`
- **Estado inicial**: Raw (sin transformaciones)
- **Formato**: CSV delimitado por el carácter `|`
- **Codificación**: UTF-8

Este archivo constituye la única fuente de entrada del pipeline. Todas las etapas posteriores trabajan sobre versiones derivadas de este dataset.

---

## Cobertura temporal

- **Periodo de referencia**: desde el año 2010 en adelante
- **Frecuencia de actualización**: **semanal**

El dataset se actualiza periódicamente incorporando nuevas semanas estadísticas, lo que lo hace adecuado para pipelines con ejecución recurrente y versionado temporal.

---

## Granularidad y variables principales

Cada registro representa información epidemiológica agregada por:

- Año estadístico
- Semana estadística
- Grupo etario
- Sexo
- Región

Variables principales incluidas:

- `ANO_ESTADISTICO`
- `SEMANA_ESTADISTICA`
- `GRUPO_EDAD`
- `SEXO`
- `REGION`
- `POBLACION`
- `MUERTES_OBS`

Este esquema es validado explícitamente durante la etapa de extracción del pipeline.

---

## Volumen aproximado

- **Cantidad de registros**: ~133.000 filas
- **Cantidad de columnas**: 7 (en estado raw)

El volumen es suficiente para representar un escenario realista de procesamiento batch sin introducir complejidad innecesaria asociada a big data.

---

## Calidad y consideraciones técnicas

El dataset raw presenta las siguientes características relevantes:

- Esquema fijo pero sin validaciones formales embebidas
- Posibles espacios en blanco en campos categóricos
- Rangos etarios expresados como texto
- Codificación numérica del sexo

Estas condiciones justifican la existencia de:

- Validaciones tempranas de estructura
- Controles de datos vacíos
- Transformaciones orientadas a normalización y enriquecimiento

---

## Uso dentro del pipeline

El dataset raw es:

1. Validado en su existencia y estructura
2. Leído desde la capa `data/raw`
3. Transformado y enriquecido
4. Persistido como dataset transformado
5. Versionado y cargado a Google Cloud Storage

En ningún caso se altera el archivo raw original, respetando el principio de inmutabilidad de la capa raw.

---

## Alcance y limitaciones

- El pipeline no modifica ni corrige valores de origen
- No se aplican inferencias estadísticas
- No se realizan imputaciones de datos

El foco está puesto en ingeniería de datos, no en análisis epidemiológico.

---

## Observación final

El uso de datos oficiales del Ministerio de Salud de Chile permite simular un escenario profesional realista, manteniendo un alto estándar de trazabilidad, reproducibilidad y buenas prácticas de ingeniería aplicadas a datos públicos.

