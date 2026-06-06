import os
from dotenv import load_dotenv
import psycopg
from psycopg import sql

load_dotenv()
DB_CONNINFO = os.getenv("POSTGRES_URI", None)
if not DB_CONNINFO:
    raise ValueError("La variable de entorno DB_CONNINFO no está definida.")

with psycopg.connect(DB_CONNINFO) as conn:
    with conn.cursor() as cursor:

        # 1. Obtener dinámicamente todas las tablas con prefijo 'news_'
        cursor.execute("""  
            SELECT table_name  
            FROM information_schema.tables  
            WHERE table_schema = 'public'  
              AND table_type = 'BASE TABLE'  
              AND table_name LIKE 'news_%'  
            ORDER BY table_name;  
        """)
        tables = [row[0] for row in cursor.fetchall()]

        # 2. Contar filas por tabla
        counts: dict[str, int] = {}
        for table in tables:
            cursor.execute(
                sql.SQL("SELECT count(url) FROM {table}").format(
                    table=sql.Identifier(table)
                )
            )
            counts[table] = cursor.fetchone()[0]

        # 3. Reporte
        print(f"{'Tabla':<30} {'Filas':>10}")
        print("-" * 42)
        for table, count in counts.items():
            print(f"{table:<30} {count:>10,}")

        total = sum(counts.values())
        print("-" * 42)
        print(f"{'TOTAL':<30} {total:>10,}")
