# StreamData Analytics — End-to-End Big Data Architecture & ETL Pipeline on AWS 🎬☁️

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)
![Amazon Athena](https://img.shields.io/badge/Amazon_Athena-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python_3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)

Arquitectura analítica completa de **Big Data e Ingeniería de Datos en Amazon Web Services (AWS)** desarrollada para **StreamData Analytics**. El proyecto abarca desde la ingesta batch de fuentes históricas, construcción de un **Data Lake multinivel en Amazon S3**, procesamiento y limpieza con **Python (Pandas & TextBlob)**, consultas SQL serverless mediante **Amazon Athena** y **AWS Glue Data Catalog**, hasta el modelado dimensional y diseño de un dashboard ejecutivo en **Microsoft Power BI**.

---

## 📌 Tabla de Contenidos
- [Contexto del Proyecto](#-contexto-del-proyecto)
- [Arquitectura de la Solución](#-arquitectura-de-la-solución)
- [Estructura del Data Lake (Amazon S3)](#-estructura-del-data-lake-amazon-s3)
- [Pipeline ETL y Procesamiento de Lenguaje Natural (NLP)](#-pipeline-etl-y-procesamiento-de-lenguaje-natural-nlp)
- [Catalogación y Consultas con Amazon Athena (SQL)](#-catalogación-y-consultas-con-amazon-athena-sql)
- [Modelo Analítico y Dashboard en Power BI](#-modelo-analítico-y-dashboard-en-power-bi)
- [Estructura del Repositorio](#-estructura-del-repositorio)
- [Instrucciones de Instalación y Ejecución](#-instrucciones-de-instalación-y-ejecución)
- [Principales Indicadores Obtenidos (KPIs)](#-principales-indicadores-obtenidos-kpis)
- [Autores](#-autores)

---

## 🏢 Contexto del Proyecto

**StreamData Analytics** es una empresa de inteligencia de datos orientada a plataformas de streaming multimedia. La organización requería centralizar información histórica del catálogo audiovisual para estudiar la composición de títulos, diversidad geográfica de producción, clasificación por edad de audiencias y la evolución temporal de incorporaciones, estableciendo las bases para un Data Lake escalable capaz de recibir futuras fuentes en streaming y APIs externas (como TMDB).

* **Dataset de origen:** Catálogo público de Netflix Movies and TV Shows (Kaggle).
* **Volumen:** 8.807 registros y 12 columnas originales (~3,24 MiB).
* **Modalidad de Ingesta:** Carga por lotes (*Batch Processing*).

---

## 🏗 Arquitectura de la Solución

El ciclo de vida del dato sigue un enfoque moderno desacoplando cómputo y almacenamiento:

```
                  [ Kaggle: netflix_titles.csv ]
                                │
                                ▼ (Carga Batch)
               ┌─────────────────────────────────┐
               │    AMAZON S3 - DATA LAKE        │
               │  s3://streamdata-equipo01-bm/   │
               └────────────────┬────────────────┘
                                │
                                ▼
               ┌─────────────────────────────────┐
               │   PIPELINE ETL LOCAL EN PYTHON  │
               │  (Pandas, Limpieza & TextBlob)  │
               └────────────────┬────────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼                                               ▼
 [ Capa PROCESSED ]                              [ Capa CURATED ]
 • netflix_titles_clean                          • netflix_sentiment_summary
 • netflix_title_countries                       • netflix_catalog_kpis
 • netflix_title_genres                                 │
        │                                               │
        └───────────────────────┬───────────────────────┘
                                ▼
               ┌─────────────────────────────────┐
               │      AWS GLUE DATA CATALOG      │
               │    Base: netflix_analytics      │
               └────────────────┬────────────────┘
                                │
                                ▼
               ┌─────────────────────────────────┐
               │     AMAZON ATHENA (SQL ANSI)    │
               │   Motor de Consultas Serverless │
               └────────────────┬────────────────┘
                                │
                                ▼ (ODBC / Import)
               ┌─────────────────────────────────┐
               │   MICROSOFT POWER BI DESKTOP    │
               │   Modelo 1:N & Dashboard KPI    │
               └─────────────────────────────────┘
```

---

## 🪣 Estructura del Data Lake (Amazon S3)

El Data Lake se diseñó siguiendo el principio de zonas lógicas para garantizar la trazabilidad y procedencia del dato:

* **`raw/`**: Almacena el dato original inmutable con particionamiento por fecha de ingesta:
  `raw/netflix/catalog/ingestion_date=2026-11-09/netflix_titles.csv`
* **`processed/`**: Datasets normalizados en formato CSV estandarizado:
  * `netflix_titles/netflix_titles_clean.csv` (8.807 filas, tabla principal).
  * `title_countries/netflix_title_countries.csv` (10.012 filas, tabla puente de países).
  * `title_genres/netflix_title_genres.csv` (19.323 filas, tabla puente de géneros).
* **`curated/`**: Datasets agregados con KPIs y métricas analíticas:
  * `netflix_sentiment_summary/` (resumen de tono léxico).
  * `netflix_catalog_kpis/` (11 métricas clave del catálogo).
* **`logs/`**: Logs de ejecución del ETL (`etl_execution.log`, `etl_summary.json`) y prefijo de salida de consultas SQL (`athena-results/`).

---

## ⚙️ Pipeline ETL y Procesamiento de Lenguaje Natural (NLP)

El script Python [`etl_netflix.py`](etl_netflix.py) automatiza:
1. **Validación de Integridad:** Verificación de tipos, unicidad de identificador (`show_id`) y detección de valores nulos.
2. **Reparación Estructural:** Corrección de anomalía en 3 registros donde la duración quedó desplazada a la columna `rating` (`s5542: 74 min`, `s5795: 84 min`, `s5814: 66 min`).
3. **Parseo Temporal:** Conversión de `date_added` al estándar ISO 8601 (`YYYY-MM-DD`), preservando las 10 fechas realmente ausentes.
4. **Normalización Relacional (1:N):** Desacoplamiento de campos multivaluados (`country` y `listed_in`) en dos tablas puente, evitando conteos duplicados.
5. **Análisis de Polaridad Léxica con TextBlob:**
   $$\text{Polaridad} \in [-1.0, +1.0]$$
   * **Positivo:** $\text{Polaridad} > 0.10$ ($37,94\%$)
   * **Neutro:** $-0.10 \le \text{Polaridad} \le 0.10$ ($42,22\%$)
   * **Negativo:** $\text{Polaridad} < -0.10$ ($19,85\%$)

---

## 🔍 Catalogación y Consultas con Amazon Athena (SQL)

Mediante el script [`athena_netflix.sql`](athena_netflix.sql), se registran tablas externas en **AWS Glue Data Catalog** utilizando el serializador `OpenCSVSerde`:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS netflix_analytics.netflix_titles_clean (
    show_id                 string,
    content_type            string,
    title                   string,
    director                string,
    cast_members            string,
    country                 string,
    primary_country         string,
    country_count           int,
    date_added              string,
    added_year              int,
    added_month             int,
    release_year            int,
    release_decade          string,
    rating                  string,
    audience_group          string,
    duration                string,
    duration_value          int,
    duration_unit           string,
    listed_in               string,
    primary_genre           string,
    genre_count             int,
    description             string,
    sentiment_polarity      double,
    sentiment_label         string,
    description_word_count  int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION 's3://streamdata-equipo01-bm-20261109/processed/netflix_titles/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

---

## 📊 Modelo Analítico y Dashboard en Power BI

El archivo [`Netflix_StreamData.pbix`](Netflix_StreamData.pbix) implementa un **modelo dimensional en estrella**:
* **Tabla de Hechos Central:** `netflix_titles_clean` (1 fila por título).
* **Tablas de Dimensión / Puente:** `netflix_title_countries` y `netflix_title_genres` con relaciones Muchos a Uno ($* : 1$) y filtro unidireccional.

### Páginas del Reporte:
1. **Resumen Ejecutivo:** Tarjetas KPI, composición Películas vs. Series (gráfico de dona), distribución de tono de sinopsis, línea temporal de incorporaciones y segmentadores dinámicos.
2. **Catálogo y Detalle:** Top 10 países de producción, Top 10 categorías, análisis de duración y matriz tabular con buscador interactivo.

### Medidas DAX Principales ([`medidas_dax.txt`](medidas_dax.txt)):
```dax
Total Títulos = DISTINCTCOUNT(netflix_titles_clean[show_id])

Porcentaje Tono Positivo = 
DIVIDE(
    CALCULATE([Total Títulos], netflix_titles_clean[sentiment_label] = "Positivo"),
    [Total Títulos],
    0
)

Títulos por País = DISTINCTCOUNT(netflix_title_countries[show_id])
```

---

## 📁 Estructura del Repositorio

```text
├── etl_netflix.py                               # Script ETL automatizado en Python
├── etl_netflix_aws.ipynb                        # Jupyter Notebook interactivo del ETL
├── athena_netflix.sql                           # DDL y consultas analíticas para Amazon Athena
├── medidas_dax.txt                              # Formulación de medidas DAX para Power BI
├── Netflix_StreamData.pbix                      # Archivo de Power BI Desktop con Dashboard
├── Informe_BigData_StreamData_AWS_Bastian_Pablo.pdf # Informe Técnico Oficial (PDF)
├── salida_netflix/                              # Artefactos generados por el pipeline
│   ├── processed/                               # CSVs normalizados para S3
│   ├── curated/                                 # Tablas agregadas y KPIs
│   └── logs/                                    # Log de ejecución y resumen JSON
└── README.md                                    # Documentación técnica del proyecto
```

---

## 🚀 Instrucciones de Instalación y Ejecución

### 1. Clonar el repositorio
```bash
git clone https://github.com/TU_USUARIO/streamdata-netflix-bigdata-aws.git
cd streamdata-netflix-bigdata-aws
```

### 2. Instalar dependencias de Python
```bash
pip install pandas openpyxl textblob
```

### 3. Ejecutar el pipeline ETL
```bash
python etl_netflix.py
```

### 4. Consultar en Amazon Athena
* Copiar y pegar las sentencias de [`athena_netflix.sql`](athena_netflix.sql) en el editor de consultas de Athena, asegurando configurar la ruta de resultados en S3 (`s3://tu-bucket/logs/athena-results/`).

### 5. Abrir el reporte en Power BI
* Abrir [`Netflix_StreamData.pbix`](Netflix_StreamData.pbix) con **Microsoft Power BI Desktop**.

---

## 📈 Principales Indicadores Obtenidos (KPIs)

* **Total de Títulos:** 8.807 ($6.131$ películas, $2.676$ series).
* **Distribución de Formato:** $69,62\%$ Películas vs $30,38\%$ Series de TV.
* **Diversidad Geográfica:** 122 países únicos de producción (Líderes: Estados Unidos, India y Reino Unido).
* **Coproducción Internacional:** $16,49\%$ de títulos multicountry.
* **Tasa de Renovación Reciente:** $61,30\%$ de los títulos con fecha conocida fueron incorporados entre 2019 y 2021.
* **Tono de Sinopsis (NLP):** $42,22\%$ Neutro, $37,94\%$ Positivo, $19,85\%$ Negativo.

---

## 👥 Autores
* **Bastián Martínez** — [LinkedIn](https://linkedin.com) | [GitHub](https://github.com)
* **Pablo Amión** — [LinkedIn](https://linkedin.com) | [GitHub](https://github.com)

*Proyecto desarrollado para la asignatura de Proyectos Big Data — Ingeniería Informática (INACAP Sede Puente Alto).*

