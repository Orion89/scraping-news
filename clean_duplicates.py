import logging
import os
import psycopg
from psycopg import sql

# Configuración del log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Reemplaza con tus credenciales si no usas variables de entorno
DB_CONNINFO = os.getenv("POSTGRES_URI", None)
if not DB_CONNINFO:
    logger.critical(
        "La variable de entorno POSTGRES_URI no está definida. Por favor, configúrala antes de ejecutar el script."
    )
    exit(1)


def remove_duplicate_hashes(
    schema: str = "public", table_prefix: str = "news_"
) -> None:
    """
    Busca todas las tablas de noticias y elimina las filas que tengan un `body_hash` duplicado,
    conservando únicamente la fila original (la más antigua según su ctid).
    """

    get_tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s 
          AND table_type = 'BASE TABLE'
          AND table_name LIKE %s;
    """

    try:
        with psycopg.connect(DB_CONNINFO) as conn:
            with conn.cursor() as cursor:

                # 1. Obtener la lista de tablas a procesar
                cursor.execute(get_tables_query, (schema, f"{table_prefix}%"))
                tables = [row[0] for row in cursor.fetchall()]

                if not tables:
                    logger.warning(
                        f"No se encontraron tablas con el prefijo '{table_prefix}'."
                    )
                    return

                logger.info(f"Iniciando purga de duplicados en {len(tables)} tablas...")

                # 2. Iterar sobre cada tabla
                for table_name in tables:

                    # La consulta SQL mágica:
                    # 'a' son las filas que vamos a borrar. 'b' son las filas que se quedan.
                    # Borramos 'a' si tiene el mismo hash que 'b' pero su ctid es mayor (es decir, se insertó después).
                    delete_query = sql.SQL("""
                        DELETE FROM {table} a
                        USING {table} b
                        WHERE a.body_hash = b.body_hash
                          AND a.ctid > b.ctid;
                    """).format(table=sql.Identifier(table_name))

                    try:
                        # Usamos transacciones por tabla. Si una falla, las demás continúan.
                        with conn.transaction():
                            cursor.execute(delete_query)

                            # rowcount nos dice exactamente cuántas filas se eliminaron
                            rows_deleted = cursor.rowcount

                            if rows_deleted > 0:
                                logger.warning(
                                    f"[LIMPIEZA] Eliminados {rows_deleted} registros duplicados en '{table_name}'."
                                )
                            else:
                                logger.info(
                                    f"[OK] La tabla '{table_name}' ya estaba limpia."
                                )

                    except Exception as e:
                        logger.error(f"[ERROR] Falló la purga en '{table_name}': {e}")

        logger.info(
            "Proceso de purga finalizado. Ya puedes aplicar las restricciones UNIQUE."
        )

    except Exception as e:
        logger.critical(f"Error fatal de conexión a la base de datos: {e}")


if __name__ == "__main__":
    remove_duplicate_hashes(table_prefix="news_")
