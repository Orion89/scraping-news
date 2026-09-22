import concurrent.futures
import gc
import hashlib
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg
import yaml
from newspaper import Article, Config
from psycopg import sql
from psycopg_pool import ConnectionPool


# ---------------------------------------------------------
# 1. CARGA DE CONFIGURACIÓN YAML
# ---------------------------------------------------------
def load_config(yaml_path="config.yaml"):
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


app_config = load_config()
script_cfg = app_config.get("script", {})
news_cfg = app_config.get("newspaper", {})

# ---------------------------------------------------------
# 2. CONFIGURACIÓN DE LOGS (CONSOLA Y ARCHIVO)
# ---------------------------------------------------------
# El FileHandler mantiene el archivo abierto y es "thread-safe",
# siendo la forma más eficiente de volcar logs masivos en disco.
log_file = script_cfg.get("log_file", "scraping_2025.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 3. SELECCIÓN DINÁMICA DEL CLIENTE HTTP
# ---------------------------------------------------------
req_client_name = script_cfg.get("request_client", "requests")
if req_client_name == "stealth_requests":
    # stealth_requests imita la API de requests perfectamente
    import stealth_requests as http_client

    logger.info("Cliente HTTP seleccionado: stealth_requests (mimics Chrome)")
else:
    import requests as http_client

    logger.info("Cliente HTTP seleccionado: requests estándar")

# ---------------------------------------------------------
# 4. VARIABLES GLOBALES Y DB
# ---------------------------------------------------------
DB_CONNINFO = os.getenv("POSTGRES_URI", None)
MAX_WORKERS = script_cfg.get("max_workers", 6)
BATCH_SIZE = script_cfg.get("batch_size", 50)
JSON_INPUT_PATH = script_cfg.get("input_json", "gap_noticias_2025.json")

# ---------------------------------------------------------
# 5. CONFIGURACIÓN DEL SCRAPER (NEWSPAPER4K)
# ---------------------------------------------------------
scraper_config = Config()
scraper_config.browser_user_agent = news_cfg.get(
    "browser_user_agent",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
scraper_config.request_timeout = news_cfg.get("request_timeout", 10)
scraper_config.memoize_articles = news_cfg.get("memoize_articles", False)
scraper_config.fetch_images = news_cfg.get("fetch_images", False)
scraper_config.language = news_cfg.get("language", "es")


def load_and_flatten_json(filepath: str, batch_size: int) -> list[tuple]:
    """
    Lee el JSON anidado y lo aplana en 'paquetes' de tareas[cite: 5].
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []
    for country, media_dict in data.items():
        country_clean = country.strip().lower()

        for media_name, years_dict in media_dict.items():
            for year, urls in years_dict.items():
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
    table_name = f"news_{country_name}"

    query = sql.SQL("""
        INSERT INTO {table} (media_name, url, date, author, body, body_hash)
        VALUES (%(media_name)s, %(url)s, %(date)s, %(authors)s, %(body_text)s, %(body_hash)s)
        ON CONFLICT (body_hash) DO NOTHING;
    """).format(table=sql.Identifier(table_name))

    params_batch = []
    stats = {"exitos": 0, "omitidos": 0, "errores": 0}

    html_results = []
    headers = {"User-Agent": scraper_config.browser_user_agent}

    def fetch_url(url):
        try:
            # Usamos el cliente HTTP dinámico (requests o stealth_requests)
            response = http_client.get(
                url, headers=headers, timeout=scraper_config.request_timeout
            )
            if response.status_code == 200:
                return url, response.text
        except Exception as e:
            # Este log quedará guardado permanentemente en el archivo de texto
            logger.error(f"Error crítico al intentar descarga {url}: {e}")
        return url, None

    with ThreadPoolExecutor(max_workers=20) as io_executor:
        html_results = list(io_executor.map(fetch_url, urls))

    for url, html_content in html_results:
        if not html_content:
            stats["errores"] += 1
            continue

        try:
            art = Article(url, config=scraper_config)
            art.download(input_html=html_content)
            art.parse()

            if not (art.is_valid_body() and art.meta_lang == "es"):
                stats["omitidos"] += 1
                del art
                continue

            body_text = art.text.strip() if art.text else None
            if not body_text:
                stats["omitidos"] += 1
                del art
                continue

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
            del art

        except Exception as e:
            stats["errores"] += 1
            logger.error(f"Error parseando HTML para {url}: {e}")
            continue

    if params_batch:
        try:
            with pool.connection() as conn, conn.cursor() as cursor:
                cursor.executemany(query, params_batch)
                conn.commit()
        except Exception as e:
            logger.error(
                f"[{country_name.upper()}] Error crítico insertando lote de {media_name} en DB: {e}"
            )
            return

    del params_batch
    del html_results
    gc.collect()

    logger.info(
        f"[{country_name.upper()} - {media_name}] Lote procesado | "
        f"Insertadas: {stats['exitos']} | Descartadas: {stats['omitidos']} | Errores: {stats['errores']}"
    )


def run_massive_extraction(json_filepath: str) -> None:
    try:
        tasks = load_and_flatten_json(json_filepath, batch_size=BATCH_SIZE)
    except Exception as e:
        logger.critical(f"Error cargando el archivo JSON de URLs: {e}")
        return

    with (
        ConnectionPool(conninfo=DB_CONNINFO, min_size=2, max_size=MAX_WORKERS) as pool,
        ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor,
    ):
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
    # Usa la ruta proveniente del YAML para iniciar el proceso
    run_massive_extraction(JSON_INPUT_PATH)
