import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import time

from dotenv import load_dotenv
import newspaper
from newspaper import Article, Config, build
import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Formato: [Fecha] [Nivel] - Mensaje
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

# Manejador 1: Consola (para ver en tiempo real)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Manejador 2: Archivo rotativo (guarda en disco, máx 5MB por archivo, mantiene 3 respaldos)
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
file_handler = RotatingFileHandler(
    f"{log_dir}/scraper.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(formatter)

# Evitar duplicados si se ejecuta múltiples veces en sesiones interactivas
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# Configuración de base de datos y workers
DB_CONNINFO = os.getenv("POSTGRES_URI", None)
if not DB_CONNINFO:
    logger.critical(
        "La variable de entorno POSTGRES_URI no está definida. Por favor, configúrala antes de ejecutar el script."
    )
    exit(1)

MAX_WORKERS = 7

# 1. Configuración global del scraper (User-Agent realista y Timeouts)
scraper_config = Config()
scraper_config.browser_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
scraper_config.request_timeout = 10
scraper_config.memoize_articles = False
scraper_config.fetch_images = False
scraper_config.language = "es"


def load_media_config(filepath: str = "medios_list.json") -> dict:
    """
    Carga el diccionario de medios desde un archivo JSON en lugar de un módulo Python.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def news_extractor_per_media(
    country_name: str,
    media_source: str,
    conn: psycopg.Connection,
    batch_size: int = 50,
) -> None:
    """
    Extrae noticias de un medio específico y las inserta en la base de datos de forma concurrente,
    limpiando los datos y gestionando la deduplicación mediante hashes y restricciones SQL.
    """

    # 1. Configuración de tabla dinámica y consulta atómica
    table_name = f"news_{country_name}"

    query = sql.SQL("""
        INSERT INTO {table} (media_name, url, date, author, body, body_hash)
        VALUES (%(media_name)s, %(url)s, %(date)s, %(authors)s, %(body_text)s, %(body_hash)s)
        ON CONFLICT (body_hash) DO NOTHING;
    """).format(table=sql.Identifier(table_name))

    params_batch = []

    # 2. Inicialización de métricas para un log limpio
    stats = {"exitos_extraidos": 0, "omitidos_validos": 0, "errores": 0}

    # Intentar acceder a la fuente principal
    try:
        source = build(media_source, config=scraper_config)
    except Exception as e:
        logger.error(
            f"[{country_name.upper()}] Error crítico accediendo a la fuente principal {media_source}: {e}"
        )
        return

    with conn.cursor() as cursor:
        for art in source.articles[
            :500
        ]:  # Limitar a los primeros 500 artículos para evitar sobrecarga
            try:
                # Descarga y parseo del artículo
                art.download()
                art.parse()

                # Validaciones estrictas del artículo
                if not (
                    art.is_valid_body()
                    and not art.is_media_news()
                    and art.meta_lang == "es"
                ):
                    stats["omitidos_validos"] += 1
                    continue

                body_text = art.text.strip() if art.text else None
                if not body_text:
                    stats["omitidos_validos"] += 1
                    continue

                # 3. Hash calculado en memoria de Python (más eficiente que en SQL)
                body_hash = hashlib.md5(body_text.encode("utf-8")).hexdigest()

                params_batch.append(
                    {
                        "media_name": (
                            art.source_url.partition("//")[-1][:149]
                            if art.source_url
                            else None
                        ),
                        "url": art.url or None,
                        "date": art.publish_date,
                        "authors": " ".join(art.authors)[:149] if art.authors else None,
                        "body_text": body_text,
                        "body_hash": body_hash,
                    }
                )

                stats["exitos_extraidos"] += 1

            except Exception as e:
                # Silenciamos el error por consola, solo sumamos a las métricas
                stats["errores"] += 1
                # Opcional: Si tienes configurado un archivo log a nivel DEBUG, puedes descomentar la siguiente línea:
                # logger.debug(f"Error extrayendo artículo específico en {media_source}: {e}")
                continue

            # 4. Inserción por lotes (Batch Insert)
            if len(params_batch) >= batch_size:
                cursor.executemany(query, params_batch)
                conn.commit()
                params_batch = []  # Limpieza de memoria (reasignación de lista)
            del art
        # Insertar los elementos restantes que no alcanzaron el múltiplo de 'batch_size'
        if params_batch:
            cursor.executemany(query, params_batch)
            conn.commit()
    del source
    gc.collect()  # Forzar recolección de basura para liberar memoria
    # 5. Log de métricas consolidado final
    logger.info(
        f"[{country_name.upper()}] Finalizado {media_source} | "
        f"Extraídas: {stats['exitos_extraidos']} | "
        f"Descartadas: {stats['omitidos_validos']} | "
        f"Errores: {stats['errores']}"
    )


def process_url(country_name: str, url: str, pool: ConnectionPool) -> None:
    """Tarea unitaria: una URL → una llamada a news_extractor_per_media."""
    with pool.connection() as conn:
        news_extractor_per_media(
            country_name=country_name,
            media_source=url,
            conn=conn,
        )


def run_extraction(json_filepath: str = "medios_list.json") -> None:

    # Carga de datos mediante JSON
    try:
        media_dict = load_media_config(json_filepath)
        logger.info(f"Archivo JSON '{json_filepath}' cargado con éxito.")
    except Exception as e:
        logger.critical(f"No se pudo cargar el archivo JSON de medios: {e}")
        return

    with ConnectionPool(conninfo=DB_CONNINFO, min_size=2, max_size=MAX_WORKERS) as pool:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            # Lanzamos todas las URLs como tareas independientes
            futures = {
                executor.submit(process_url, country, url, pool): (country, url)
                for country, urls in media_dict.items()
                for url in urls
            }

            for future in as_completed(futures):
                country, url = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Error procesando {url} del país {country}: {exc}")


if __name__ == "__main__":
    # Suponiendo que has convertido tu diccionaro a un archivo llamado 'medios_list.json'
    run_extraction("./data/medios_list.json")
