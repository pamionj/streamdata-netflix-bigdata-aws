"""
=============================================================================
PROYECTO BIG DATA - EVALUACIÓN SUMATIVA 3
Caso Empresarial: StreamData Analytics
Dataset: Netflix Movies and TV Shows (netflix_titles.csv)
Autores: Bastián Martínez & Pablo Amión
Institución: INACAP - Sede Puente Alto

Script ETL: etl_netflix.py
Descripción:
  1. Extracción y validación del archivo original raw netflix_titles.csv.
  2. Limpieza de datos (nulos, formatos, anomalías de duración en rating).
  3. Enriquecimiento analítico y análisis léxico/sentimiento con TextBlob.
  4. Generación de tablas puente normalizadas (países y géneros).
  5. Exportación de datasets estructurados para las capas Processed y Curated
     del Data Lake en Amazon S3 y consultas serverless en Amazon Athena.
=============================================================================
"""

import os
import sys
import json
import logging
import datetime
import warnings
import pandas as pd
from textblob import TextBlob

warnings.filterwarnings("ignore")

# Configuración de Logging y directorios
os.makedirs("salida_netflix/logs", exist_ok=True)
os.makedirs("salida_netflix/processed", exist_ok=True)
os.makedirs("salida_netflix/curated", exist_ok=True)
os.makedirs("salida_netflix/curated/visuals", exist_ok=True)

log_path = os.path.join("salida_netflix/logs", "etl_execution.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_etl():
    logging.info("=============================================================")
    logging.info("INICIO DEL PROCESO ETL - STREAMDATA ANALYTICS (NETFLIX DATASET)")
    logging.info("=============================================================")

    # 1. EXTRACCIÓN Y VALIDACIÓN DE FUENTE
    csv_input = "netflix_titles.csv"
    if not os.path.exists(csv_input):
        logging.error(f"No se encontró el archivo de entrada '{csv_input}'.")
        return

    file_size_bytes = os.path.getsize(csv_input)
    logging.info(f"Archivo detectado: {csv_input} ({file_size_bytes} bytes / {file_size_bytes/(1024*1024):.2f} MiB)")

    df = pd.read_csv(csv_input, encoding="utf-8")
    initial_rows, initial_cols = df.shape
    logging.info(f"Registros leídos: {initial_rows} filas, {initial_cols} columnas.")

    # Validar columnas requeridas
    expected_cols = [
        "show_id", "type", "title", "director", "cast", "country",
        "date_added", "release_year", "rating", "duration", "listed_in", "description"
    ]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        logging.error(f"Columnas faltantes en el CSV: {missing_cols}")
        return
    logging.info("Validación de esquema: 12 columnas obligatorias presentes y correctas.")

    # 2. LIMPIEZA Y REPARACIÓN ESTRUCTURAL
    # Normalizar espacios en texto
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype).startswith("str"):
            df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

    # Reemplazar 'nan' y cadenas vacías por None temporal
    df.replace({"nan": None, "": None}, inplace=True)

    # Identificadores duplicados o nulos
    null_ids = int(df["show_id"].isna().sum())
    dup_ids = int(df.duplicated(subset=["show_id"]).sum())
    logging.info(f"Validación de identificadores (show_id): {null_ids} nulos, {dup_ids} duplicados.")

    # Reparar anomalía conocida: duraciones desplazadas al campo rating (s5542, s5795, s5814)
    anomaly_mask = df["rating"].astype(str).str.contains(r"\d+\s*min", na=False) & df["duration"].isna()
    anomalies_count = int(anomaly_mask.sum())
    if anomalies_count > 0:
        logging.info(f"Reparando anomalía de rating desplazado a duration ({anomalies_count} filas)...")
        df.loc[anomaly_mask, "duration"] = df.loc[anomaly_mask, "rating"]
        df.loc[anomaly_mask, "rating"] = "No informado"

    # Estandarización de faltantes categóricos
    categorical_cols = ["director", "cast", "country", "rating", "duration"]
    for col in categorical_cols:
        df[col] = df[col].fillna("No informado")

    # Conversión y parsing de fechas date_added ("Month DD, YYYY" -> "YYYY-MM-DD")
    raw_dates = df["date_added"].astype(str).str.strip()
    parsed_dates = pd.to_datetime(raw_dates, format="mixed", errors="coerce")
    null_dates = int(parsed_dates.isna().sum())
    logging.info(f"Fechas parseadas correctamente. Fechas ausentes preservadas como nulas: {null_dates}")

    df["date_added_clean"] = parsed_dates.dt.strftime("%Y-%m-%d")
    df["added_year"] = parsed_dates.dt.year.astype("Int64")
    df["added_month"] = parsed_dates.dt.month.astype("Int64")

    # 3. CAMPOS DERIVADOS Y ANÁLISIS DE TEXTO / SENTIMIENTO
    # Renombrar type a content_type
    df = df.rename(columns={"type": "content_type"})

    # Atributos de país y género
    def get_first_elem(val):
        if not val or val == "No informado":
            return "No informado"
        return val.split(",")[0].strip()

    def get_count_elems(val):
        if not val or val == "No informado":
            return 0
        return len([x for x in val.split(",") if x.strip()])

    df["primary_country"] = df["country"].apply(get_first_elem)
    df["country_count"] = df["country"].apply(get_count_elems)

    df["primary_genre"] = df["listed_in"].apply(get_first_elem)
    df["genre_count"] = df["listed_in"].apply(get_count_elems)

    # Década de lanzamiento
    df["release_decade"] = (df["release_year"] // 10 * 10).astype(str) + "s"

    # Grupos de audiencia
    def map_audience(rating):
        r = str(rating).upper().strip()
        if r in ["TV-MA", "R", "NC-17", "UR"]:
            return "Adultos"
        elif r in ["PG-13", "TV-14"]:
            return "Adolescentes"
        elif r in ["PG", "TV-PG"]:
            return "Supervisión parental"
        elif r in ["TV-Y7", "TV-Y7-FV"]:
            return "Infantil"
        elif r in ["G", "TV-G", "TV-Y"]:
            return "General"
        else:
            return "Sin clasificación"

    df["audience_group"] = df["rating"].apply(map_audience)

    # Duración numérica y unidad
    def parse_duration(d_str):
        if not d_str or d_str == "No informado":
            return 0, "No informado"
        parts = str(d_str).split()
        val = int(parts[0]) if parts[0].isdigit() else 0
        unit = "min" if "min" in d_str.lower() else "season"
        return val, unit

    dur_parsed = df["duration"].apply(parse_duration)
    df["duration_value"] = [p[0] for p in dur_parsed]
    df["duration_unit"] = [p[1] for p in dur_parsed]

    # Análisis de Sentimiento / Polaridad léxica con TextBlob sobre description
    logging.info("Calculando polaridad léxica sobre description con TextBlob...")
    def analyze_sentiment(text):
        if not text or text == "No informado":
            return 0.0, "Neutro", 0
        t_clean = str(text)
        blob = TextBlob(t_clean)
        pol = blob.sentiment.polarity
        word_count = len(t_clean.split())
        
        # Clasificación según umbrales del informe (>0.10 Positivo, <-0.10 Negativo, resto Neutro)
        if pol > 0.10:
            label = "Positivo"
        elif pol < -0.10:
            label = "Negativo"
        else:
            label = "Neutro"
        return round(pol, 4), label, word_count

    sent_results = df["description"].apply(analyze_sentiment)
    df["sentiment_polarity"] = [r[0] for r in sent_results]
    df["sentiment_label"] = [r[1] for r in sent_results]
    df["description_word_count"] = [r[2] for r in sent_results]

    sent_counts = df["sentiment_label"].value_counts()
    logging.info(f"Distribución de tono de sinopsis: {dict(sent_counts)}")

    # 4. TABLAS PUENTE NORMALIZADAS (PAÍSES Y GÉNEROS)
    # Tabla Puente: netflix_title_countries (excluyendo 'No informado' para relaciones válidas)
    country_rows = []
    for _, row in df[df["country"] != "No informado"].iterrows():
        for c in str(row["country"]).split(","):
            c_clean = c.strip()
            if c_clean:
                country_rows.append({"show_id": row["show_id"], "country_name": c_clean})
    df_countries = pd.DataFrame(country_rows)

    # Tabla Puente: netflix_title_genres
    genre_rows = []
    for _, row in df.iterrows():
        g_val = str(row["listed_in"]) if pd.notna(row["listed_in"]) else "No informado"
        for g in g_val.split(","):
            g_clean = g.strip()
            if g_clean:
                genre_rows.append({"show_id": row["show_id"], "genre": g_clean})
    df_genres = pd.DataFrame(genre_rows)

    logging.info(f"Tabla puente países generada: {len(df_countries)} filas, {df_countries['country_name'].nunique()} países distintos.")
    logging.info(f"Tabla puente géneros generada: {len(df_genres)} filas, {df_genres['genre'].nunique()} géneros distintos.")

    # 5. TABLAS AGREGADAS Y KPIS (CAPA CURATED)
    df_sent_summary = pd.DataFrame([
        {"sentiment_label": "Positivo", "title_count": int(sent_counts.get("Positivo", 0)), "percentage": round(float(sent_counts.get("Positivo", 0)) / len(df) * 100, 2)},
        {"sentiment_label": "Neutro", "title_count": int(sent_counts.get("Neutro", 0)), "percentage": round(float(sent_counts.get("Neutro", 0)) / len(df) * 100, 2)},
        {"sentiment_label": "Negativo", "title_count": int(sent_counts.get("Negativo", 0)), "percentage": round(float(sent_counts.get("Negativo", 0)) / len(df) * 100, 2)},
    ])

    # Tabla de 11 KPIs oficiales
    movies_count = int((df["content_type"] == "Movie").sum())
    tv_count = int((df["content_type"] == "TV Show").sum())
    recent_titles = int(df["added_year"].isin([2019, 2020, 2021]).sum())
    valid_dates_count = len(df) - null_dates
    multicountry_titles = int((df["country_count"] > 1).sum())
    titles_with_country = int((df["country"] != "No informado").sum())

    kpi_records = [
        {"metric_name": "Total Titulos", "metric_value": str(len(df)), "metric_unit": "Unidades", "definition": "Total de titulos unicos catalogados"},
        {"metric_name": "Total Peliculas", "metric_value": str(movies_count), "metric_unit": "Unidades", "definition": "Titulos clasificados como Movie"},
        {"metric_name": "Total Series", "metric_value": str(tv_count), "metric_unit": "Unidades", "definition": "Titulos clasificados como TV Show"},
        {"metric_name": "Proporcion Peliculas", "metric_value": f"{movies_count/len(df)*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Porcentaje de peliculas sobre el total"},
        {"metric_name": "Proporcion Series", "metric_value": f"{tv_count/len(df)*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Porcentaje de series sobre el total"},
        {"metric_name": "Paises Unicos", "metric_value": str(df_countries['country_name'].nunique()), "metric_unit": "Paises", "definition": "Cantidad de paises distintos en produccion"},
        {"metric_name": "Generos Unicos", "metric_value": str(df_genres['genre'].nunique()), "metric_unit": "Categorias", "definition": "Cantidad de categorias listadas distintas"},
        {"metric_name": "Tono Positivo Sinopsis", "metric_value": f"{sent_counts.get('Positivo', 0)/len(df)*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Sinopsis con polaridad TextBlob > 0.10"},
        {"metric_name": "Tono Neutro Sinopsis", "metric_value": f"{sent_counts.get('Neutro', 0)/len(df)*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Sinopsis con polaridad TextBlob entre -0.10 y 0.10"},
        {"metric_name": "Tono Negativo Sinopsis", "metric_value": f"{sent_counts.get('Negativo', 0)/len(df)*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Sinopsis con polaridad TextBlob < -0.10"},
        {"metric_name": "Indice Renovacion Reciente", "metric_value": f"{recent_titles/valid_dates_count*100:.2f}%", "metric_unit": "Porcentaje", "definition": "Titulos incorporados en 2019-2021 sobre fechas validas"}
    ]
    df_kpis = pd.DataFrame(kpi_records)

    # 6. EXPORTACIÓN DE ARCHIVOS PARA S3 / PROCESSED Y CURATED
    cols_clean_order = [
        "show_id", "content_type", "title", "director", "cast", "country",
        "primary_country", "country_count", "date_added_clean", "added_year", "added_month",
        "release_year", "release_decade", "rating", "audience_group", "duration",
        "duration_value", "duration_unit", "listed_in", "primary_genre", "genre_count",
        "description", "sentiment_polarity", "sentiment_label", "description_word_count"
    ]
    df_clean = df[cols_clean_order].rename(columns={"date_added_clean": "date_added", "cast": "cast_members"})

    path_clean = "salida_netflix/processed/netflix_titles_clean.csv"
    path_countries = "salida_netflix/processed/netflix_title_countries.csv"
    path_genres = "salida_netflix/processed/netflix_title_genres.csv"
    path_sentiment = "salida_netflix/curated/netflix_sentiment_summary.csv"
    path_kpis = "salida_netflix/curated/netflix_catalog_kpis.csv"

    df_clean.to_csv(path_clean, index=False, encoding="utf-8")
    df_countries.to_csv(path_countries, index=False, encoding="utf-8")
    df_genres.to_csv(path_genres, index=False, encoding="utf-8")
    df_sent_summary.to_csv(path_sentiment, index=False, encoding="utf-8")
    df_kpis.to_csv(path_kpis, index=False, encoding="utf-8")

    logging.info(f"Guardado: {path_clean} ({os.path.getsize(path_clean)} bytes)")
    logging.info(f"Guardado: {path_countries} ({os.path.getsize(path_countries)} bytes)")
    logging.info(f"Guardado: {path_genres} ({os.path.getsize(path_genres)} bytes)")
    logging.info(f"Guardado: {path_sentiment} ({os.path.getsize(path_sentiment)} bytes)")
    logging.info(f"Guardado: {path_kpis} ({os.path.getsize(path_kpis)} bytes)")

    # 7. GENERACIÓN DE RESUMEN JSON
    summary_data = {
        "fecha_ejecucion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "archivo_origen": csv_input,
        "registros_totales": len(df_clean),
        "peliculas": movies_count,
        "series": tv_count,
        "relaciones_pais": len(df_countries),
        "paises_distintos": int(df_countries["country_name"].nunique()),
        "relaciones_genero": len(df_genres),
        "generos_distintos": int(df_genres["genre"].nunique()),
        "fechas_nulas": null_dates,
        "duraciones_reparadas": anomalies_count,
        "polaridad_positiva": int(sent_counts.get("Positivo", 0)),
        "polaridad_neutra": int(sent_counts.get("Neutro", 0)),
        "polaridad_negativa": int(sent_counts.get("Negativo", 0)),
        "estado": "EXITO_VALIDADO"
    }

    summary_path = "salida_netflix/logs/etl_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=4, ensure_ascii=False)
    logging.info(f"Resumen JSON generado en {summary_path}")

    logging.info("=============================================================")
    logging.info("PROCESO ETL FINALIZADO CON ÉXITO")
    logging.info("=============================================================")

if __name__ == "__main__":
    run_etl()
