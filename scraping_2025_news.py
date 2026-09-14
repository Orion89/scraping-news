import concurrent.futures
import gc
import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg
from newspaper import Article, Config
from psycopg import sql
from psycopg_pool import ConnectionPool

# Configuración básica de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_CONNINFO = os.getenv(
    "POSTGRES_URI", "postgresql://postgres:0rioN-689@localhost:5432/news"
)

# LIMITAMOS a 5 workers máximo para proteger la memoria RAM al procesar 200k+ enlaces
MAX_WORKERS = 5
BATCH_SIZE = 50

# Configuración del scraper: Crucial tener un timeout bajo para no colgar hilos con webs muertas
scraper_config = Config()
scraper_config.browser_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
scraper_config.request_timeout = 8
scraper_config.memoize_articles = False
scraper_config.fetch_images = False
scraper_config.language = "es"


def load_and_flatten_json(filepath: str, batch_size: int) -> list[tuple]:
    """
    Lee el JSON anidado y lo aplana en 'paquetes' de tareas.
    Retorna una lista de tuplas: (pais, nombre_medio, lista_de_urls_del_lote)
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []
    # Iterar sobre el JSON: data[país][medio][año] = [urls...]
    for country, media_dict in data.items():
        # Normalizamos el nombre del país para que coincida con tus tablas (ej. "Argentina" -> "argentina")
        country_clean = country.strip().lower()

        for media_name, years_dict in media_dict.items():
            for year, urls in years_dict.items():
                # Dividir la lista de miles de URLs en pequeños lotes (chunks)
                for i in range(0, len(urls), batch_size):
                    batch_urls = urls[i : i + batch_size]
                    tasks.append((country_clean, media_name, batch_urls))

    logger.info(
        f"JSON aplanado: Se generaron {len(tasks)} lotes de trabajo (aprox {batch_size} URLs c/u)."
    )
    return tasks


def process_url_batch(
    country_name: str, media_name: str, urls: list[str], pool: ConnectionPool
) -> None:
    """
    Toma un lote de URLs, instancia los artículos, extrae los datos e inserta el lote entero en DB.
    """
    table_name = f"news_{country_name}"

    query = sql.SQL("""
        INSERT INTO {table} (media_name, url, date, author, body, body_hash)
        VALUES (%(media_name)s, %(url)s, %(date)s, %(authors)s, %(body_text)s, %(body_hash)s)
        ON CONFLICT (body_hash) DO NOTHING;
    """).format(table=sql.Identifier(table_name))

    params_batch = []
    stats = {"exitos": 0, "omitidos": 0, "errores": 0}

    # 1. Procesamiento de scraping en memoria
    for url in urls:
        try:
            # Usamos Article directamente, ya que tenemos la URL final
            art = Article(url, config=scraper_config)
            art.download()
            art.parse()

            # Validación de contenido
            if not (art.is_valid_body() and art.meta_lang == "es"):
                stats["omitidos"] += 1
                del art
                continue

            body_text = art.text.strip() if art.text else None
            if not body_text:
                stats["omitidos"] += 1
                del art
                continue

            # Generación de Hash
            body_hash = hashlib.md5(body_text.encode("utf-8")).hexdigest()

            params_batch.append(
                {
                    "media_name": media_name,
                    "url": art.url or url,
                    "date": art.publish_date,
                    "authors": " ".join(art.authors)[:149] if art.authors else None,
                    "body_text": body_text,
                    "body_hash": body_hash,
                }
            )

            stats["exitos"] += 1

            # LIBERACIÓN DE MEMORIA INMEDIATA: Destruye el objeto HTML pesado
            del art

        except Exception as e:
            stats["errores"] += 1
            continue

    # 2. Inserción masiva en base de datos si hubo éxitos
    if params_batch:
        try:
            # Adquiere conexión del pool solo por la fracción de segundo necesaria para el insert
            with pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, params_batch)
                    conn.commit()
        except Exception as e:
            logger.error(
                f"[{country_name.upper()}] Error crítico insertando lote de {media_name} en DB: {e}"
            )
            # Si falla la base de datos, no sumamos al GC y salimos
            return

    # 3. Forzar limpieza profunda de RAM tras procesar el lote completo
    del params_batch
    gc.collect()

    # Log de rendimiento del lote
    logger.info(
        f"[{country_name.upper()} - {media_name}] Lote procesado | "
        f"Insertadas: {stats['exitos']} | Descartadas: {stats['omitidos']} | Errores: {stats['errores']}"
    )


def run_massive_extraction(json_filepath: str) -> None:
    # 1. Cargar y preparar los datos
    try:
        tasks = load_and_flatten_json(json_filepath, batch_size=BATCH_SIZE)
    except Exception as e:
        logger.critical(f"Error cargando el archivo JSON de URLs: {e}")
        return

    # 2. Iniciar el Pool de base de datos y el Pool de Hilos
    with ConnectionPool(conninfo=DB_CONNINFO, min_size=2, max_size=MAX_WORKERS) as pool:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Mapeamos los futures para gestionar errores
            futures = {
                executor.submit(process_url_batch, country, media, urls, pool): (
                    country,
                    media,
                )
                for country, media, urls in tasks
            }

            for future in as_completed(futures):
                country, media = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(
                        f"[{country.upper()}] Falló catastróficamente un worker para el medio {media}: {exc}"
                    )

    logger.info("¡Extracción de gap 2025 finalizada por completo!")


if __name__ == "__main__":
    # Sustituye por el nombre real de tu archivo JSON
    run_massive_extraction("gap_noticias_2025.json")
