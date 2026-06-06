import logging
import os
import psycopg
from psycopg import sql
from psycopg.errors import DuplicateObject, UndefinedColumn, UniqueViolation

# Configuración básica de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Usar variable de entorno o tu cadena de conexión actual
DB_CONNINFO = os.getenv("POSTGRES_URI", None)
if not DB_CONNINFO:
    logger.critical(
        "La variable de entorno POSTGRES_URI no está definida. Por favor, configúrala antes de ejecutar el script."
    )
    exit(1)


def apply_unique_constraints(
    schema: str = "public", table_prefix: str = "news_"
) -> None:
    """
    Busca todas las tablas en un esquema específico que coincidan con un prefijo
    y les aplica una restricción UNIQUE a la columna body_hash.
    """

    # Consulta para obtener los nombres de las tablas
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

                # 1. Extraer los nombres de las tablas dinámicamente
                # Filtramos por prefijo (ej. 'news_%') para evitar alterar tablas ajenas a nuestro scraper
                cursor.execute(get_tables_query, (schema, f"{table_prefix}%"))
                tables = [row[0] for row in cursor.fetchall()]

                if not tables:
                    logger.warning(
                        f"No se encontraron tablas con el prefijo '{table_prefix}' en el esquema '{schema}'."
                    )
                    return

                logger.info(
                    f"Se encontraron {len(tables)} tablas. Iniciando proceso de alteración..."
                )

                # 2. Iterar sobre cada tabla encontrada
                for table_name in tables:
                    constraint_name = f"unique_body_hash_{table_name}"

                    # Construcción segura de la consulta DDL usando psycopg.sql
                    alter_query = sql.SQL(
                        "ALTER TABLE {table} ADD CONSTRAINT {constraint} UNIQUE (body_hash);"
                    ).format(
                        table=sql.Identifier(table_name),
                        constraint=sql.Identifier(constraint_name),
                    )

                    try:
                        # Usamos un Savepoint implícito (o manejando transacciones por tabla)
                        # para que un error en una tabla no arruine toda la conexión.
                        with conn.transaction():
                            cursor.execute(alter_query)
                            logger.info(
                                f"[ÉXITO] Constraint '{constraint_name}' añadido a la tabla '{table_name}'."
                            )

                    except DuplicateObject:
                        # La restricción ya existe, lo cual es normal si ejecutamos el script más de una vez
                        logger.info(
                            f"[OMITIDO] El constraint '{constraint_name}' ya existe en '{table_name}'."
                        )

                    except UndefinedColumn:
                        # La tabla no tiene la columna body_hash
                        logger.error(
                            f"[ERROR] La tabla '{table_name}' no tiene la columna 'body_hash'."
                        )

                    except UniqueViolation:
                        # ERROR CRÍTICO COMÚN: Ya hay datos duplicados en la base de datos
                        logger.error(
                            f"[ERROR] No se pudo añadir el constraint en '{table_name}' porque "
                            f"¡YA EXISTEN valores duplicados en la columna body_hash! "
                            f"Debes limpiar los duplicados antes de aplicar esta restricción."
                        )
                    except Exception as e:
                        logger.error(
                            f"[ERROR INESPERADO] Falló la alteración en '{table_name}': {e}"
                        )

    except Exception as e:
        logger.critical(f"No se pudo conectar a la base de datos o error fatal: {e}")


if __name__ == "__main__":
    # Ejecuta la función buscando todas las tablas que empiecen por "news_"
    apply_unique_constraints(table_prefix="news_")
