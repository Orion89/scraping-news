import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Garantizar que POSTGRES_URI esté presente antes de importar scraping_news
os.environ.setdefault("POSTGRES_URI", "postgresql://test_user:test_pass@localhost:5432/test_db")

# Mockear dependencias externas si no están en el entorno de pruebas
for mod_name in ["dotenv", "newspaper", "psycopg", "psycopg.sql", "psycopg_pool"]:
    if mod_name not in sys.modules:
        try:
            __import__(mod_name)
        except ImportError:
            sys.modules[mod_name] = MagicMock()

from scraping_news import (
    FAILED_DETAILS_FILE,
    SKIP_ERROR_TYPES,
    load_failed_media_to_skip,
    run_extraction,
)


class TestSkipFailedMedia(unittest.TestCase):

    def test_skip_error_types_contents(self):
        """Verifica que el set de tipos de error a omitir contenga exactamente los 3 requeridos."""
        self.assertEqual(
            SKIP_ERROR_TYPES,
            {
                "Fallo total artículos",
                "Error crítico build()",
                "Excepción no controlada",
            },
        )

    def test_load_failed_media_missing_file(self):
        """Verifica que si el archivo no existe, retorna un diccionario vacío sin fallar."""
        missing_path = "archivo_inexistente_12345.txt"
        result = load_failed_media_to_skip(missing_path)
        self.assertEqual(result, {})

    def test_load_failed_media_synthetic(self):
        """Verifica el parseo con archivo sintético, tipos de error y líneas multilínea/malformadas."""
        synthetic_content = """# Comentario inicial
2026-10-25 11:26:17 | [CHILE] | https://medio-fallo-articulos.com/ | Fallo total artículos | Error de artículos
2026-10-25 11:46:00 | [CHILE] | https://medio-build-error.com/ | Error crítico build() | Error 403
2026-10-25 11:27:26 | [CHILE] | https://medio-sin-articulos.com/ | Sin artículos detectados | 0 artículos
2026-10-26 05:15:01 | [CIENCIA] | https://medio-excepcion.com/ | Excepción no controlada | no existe tabla
LINE 2:         INSERT INTO "news_ciencia"
                    ^
2026-10-26 05:15:02 | [CHILE] | https://medio-build-error.com/ | Fallo total artículos | Otro error más
"""
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as f:
            f.write(synthetic_content)
            temp_path = f.name

        try:
            result = load_failed_media_to_skip(temp_path)

            # Debe incluir los 3 medios con errores críticos
            self.assertIn("https://medio-fallo-articulos.com/", result)
            self.assertEqual(result["https://medio-fallo-articulos.com/"], "Fallo total artículos")

            self.assertIn("https://medio-build-error.com/", result)
            self.assertIn("Error crítico build()", result["https://medio-build-error.com/"])
            self.assertIn("Fallo total artículos", result["https://medio-build-error.com/"])

            self.assertIn("https://medio-excepcion.com/", result)
            self.assertEqual(result["https://medio-excepcion.com/"], "Excepción no controlada")

            # NO debe incluir el medio con 'Sin artículos detectados'
            self.assertNotIn("https://medio-sin-articulos.com/", result)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_load_failed_media_real_file(self):
        """Verifica la carga del archivo real failed_media/failed_media_details.txt."""
        self.assertTrue(os.path.exists(FAILED_DETAILS_FILE), f"No se encontró {FAILED_DETAILS_FILE}")

        result = load_failed_media_to_skip(FAILED_DETAILS_FILE)

        # Debe contener exactamente 293 medios únicos a omitir
        self.assertEqual(len(result), 293)

        # Medios conocidos que deben estar
        self.assertIn("https://www.encancha.cl/", result)
        self.assertIn("https://www.chanarcillo.cl/", result)
        self.assertIn("https://www.agenciasinc.es/", result)

        # Medios con 'Sin artículos detectados' no deben estar
        self.assertNotIn("http://www.soyantofagasta.cl/", result)
        self.assertNotIn("http://www.soycalama.cl/", result)
        self.assertNotIn("https://www.opinionsur.cl/", result)

    def test_filter_tasks_with_real_data(self):
        """Verifica que la integración con data/medios_list.json filtre exactamente las entradas esperadas."""
        with open("data/medios_list.json", "r", encoding="utf-8") as f:
            media_dict = json.load(f)

        failed_media_to_skip = load_failed_media_to_skip(FAILED_DETAILS_FILE)

        tasks = []
        skipped_count = 0
        for country, urls in media_dict.items():
            for url in urls:
                cleaned_url = url.strip()
                if cleaned_url in failed_media_to_skip:
                    skipped_count += 1
                else:
                    tasks.append((country, cleaned_url))

        total_media = sum(len(urls) for urls in media_dict.values())
        self.assertEqual(total_media, 1525)
        self.assertEqual(skipped_count, 301)
        self.assertEqual(len(tasks), 1224)

    @patch("scraping_news.ConnectionPool")
    @patch("scraping_news.ThreadPoolExecutor")
    def test_run_extraction_only_submits_non_skipped(self, mock_executor_cls, mock_pool_cls):
        """Verifica que run_extraction omita los medios marcados y sólo envíe las tareas válidas al executor."""
        import concurrent.futures

        mock_pool = MagicMock()
        mock_pool_cls.return_value.__enter__.return_value = mock_pool

        mock_executor = MagicMock()
        mock_executor_cls.return_value.__enter__.return_value = mock_executor
        
        # Usar un Future real completado para que as_completed no se quede esperando
        completed_future = concurrent.futures.Future()
        completed_future.set_result(None)
        mock_executor.submit.return_value = completed_future

        sample_dict = {
            "chile": [
                "https://www.emol.com/",           # Válido
                "https://www.encancha.cl/",        # Fallido en archivo real (Fallo total artículos)
            ]
        }

        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as f:
            json.dump(sample_dict, f)
            temp_json_path = f.name

        try:
            run_extraction(temp_json_path)

            # El executor solo debe haber recibido https://www.emol.com/
            self.assertEqual(mock_executor.submit.call_count, 1)
            submitted_call = mock_executor.submit.call_args_list[0]
            # Args: (process_url, country, url, pool)
            self.assertEqual(submitted_call[0][1], "chile")
            self.assertEqual(submitted_call[0][2], "https://www.emol.com/")
        finally:
            if os.path.exists(temp_json_path):
                os.remove(temp_json_path)


if __name__ == "__main__":
    unittest.main()
