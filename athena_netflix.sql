-- =============================================================================
-- PROYECTO BIG DATA - EVALUACIÓN SUMATIVA 3
-- Caso Empresarial: StreamData Analytics
-- Plataforma: AWS Academy / Amazon Athena (AWS Glue Data Catalog)
-- Bucket S3: s3://streamdata-equipo01-bm-20261109/
-- Base de Datos: netflix_analytics
-- Autores: Bastián Martínez & Pablo Amión
-- Institución: INACAP - Sede Puente Alto
-- =============================================================================

-- =============================================================================
-- 1. CREACIÓN DE LA BASE DE DATOS EN AWS GLUE DATA CATALOG
-- =============================================================================
CREATE DATABASE IF NOT EXISTS netflix_analytics
COMMENT 'Base analítica para el catálogo de Netflix - StreamData Analytics';

-- Establecer el contexto de la base de datos
USE netflix_analytics;

-- =============================================================================
-- 2. DDL DE TABLAS EXTERNAS (ZONA PROCESSED)
-- =============================================================================

-- 2.1 Tabla Central: netflix_titles_clean (Granularidad: 1 fila por título)
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

-- 2.2 Tabla Puente: netflix_title_countries (Granularidad: 1 fila por título y país)
CREATE EXTERNAL TABLE IF NOT EXISTS netflix_analytics.netflix_title_countries (
    show_id                 string,
    country_name            string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION 's3://streamdata-equipo01-bm-20261109/processed/title_countries/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2.3 Tabla Puente: netflix_title_genres (Granularidad: 1 fila por título y género)
CREATE EXTERNAL TABLE IF NOT EXISTS netflix_analytics.netflix_title_genres (
    show_id                 string,
    genre                   string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION 's3://streamdata-equipo01-bm-20261109/processed/title_genres/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- =============================================================================
-- 3. DDL DE TABLAS EXTERNAS (ZONA CURATED)
-- =============================================================================

-- 3.1 Tabla Resumen de Polaridad / Tono Léxico de Sinopsis
CREATE EXTERNAL TABLE IF NOT EXISTS netflix_analytics.netflix_sentiment_summary (
    sentiment_label         string,
    title_count             int,
    percentage              double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION 's3://streamdata-equipo01-bm-20261109/curated/netflix_sentiment_summary/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3.2 Tabla de Indicadores Clave de Rendimiento (KPIs de Catálogo)
CREATE EXTERNAL TABLE IF NOT EXISTS netflix_analytics.netflix_catalog_kpis (
    metric_name             string,
    metric_value            string,
    metric_unit             string,
    definition              string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION 's3://streamdata-equipo01-bm-20261109/curated/netflix_catalog_kpis/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- =============================================================================
-- 4. CONSULTAS SQL DE VALIDACIÓN Y CONTROL ANALÍTICO
-- =============================================================================

-- Consulta 1: Conteo total de títulos únicos y validación de identificador
SELECT 
    COUNT(*) AS total_registros,
    COUNT(DISTINCT show_id) AS total_titulos_unicos
FROM netflix_analytics.netflix_titles_clean;
-- Resultado esperado: 8807 | 8807

-- Consulta 2: Distribución por tipo de contenido (Películas vs Series)
SELECT 
    content_type,
    COUNT(*) AS cantidad_titulos,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM netflix_analytics.netflix_titles_clean
GROUP BY content_type;
-- Resultado esperado: Movie = 6131 (69.62%) | TV Show = 2676 (30.38%)

-- Consulta 3: Validación de tablas puente (países y géneros)
SELECT 
    'Paises' AS dimension,
    COUNT(*) AS total_relaciones,
    COUNT(DISTINCT country_name) AS elementos_distintos
FROM netflix_analytics.netflix_title_countries
UNION ALL
SELECT 
    'Generos' AS dimension,
    COUNT(*) AS total_relaciones,
    COUNT(DISTINCT genre) AS elementos_distintos
FROM netflix_analytics.netflix_title_genres;
-- Resultado esperado: Paises = 10012 relaciones, 122 distintos | Generos = 19323 relaciones, 42 distintos

-- Consulta 4: Distribución de tono de sinopsis (TextBlob NLP)
SELECT 
    sentiment_label,
    COUNT(*) AS total_sinopsis,
    ROUND(COUNT(*) * 100.0 / 8807, 2) AS porcentaje_catalogo
FROM netflix_analytics.netflix_titles_clean
GROUP BY sentiment_label
ORDER BY total_sinopsis DESC;
-- Resultado esperado: Neutro = 3718 (42.22%) | Positivo = 3342 (37.95%) | Negativo = 1747 (19.84%)

-- Consulta 5: Verificación de fechas ausentes preservadas (calidad del dato)
SELECT 
    COUNT(*) AS fechas_incorporacion_nulas
FROM netflix_analytics.netflix_titles_clean
WHERE date_added IS NULL OR date_added = 'No informado' OR date_added = '';
-- Resultado esperado: 10

-- Consulta 6: Top 10 países con mayor presencia en el catálogo
SELECT 
    country_name,
    COUNT(DISTINCT show_id) AS total_titulos
FROM netflix_analytics.netflix_title_countries
GROUP BY country_name
ORDER BY total_titulos DESC
LIMIT 10;
-- Principales: United States, India, United Kingdom, Canada, France...

-- Consulta 7: Top 10 géneros más frecuentes
SELECT 
    genre,
    COUNT(DISTINCT show_id) AS total_titulos
FROM netflix_analytics.netflix_title_genres
GROUP BY genre
ORDER BY total_titulos DESC
LIMIT 10;
-- Principales: International Movies, Dramas, Comedies, International TV Shows...

-- Consulta 8: Verificación de los 11 KPIs calculados en la capa Curated
SELECT 
    metric_name,
    metric_value,
    metric_unit,
    definition
FROM netflix_analytics.netflix_catalog_kpis;

