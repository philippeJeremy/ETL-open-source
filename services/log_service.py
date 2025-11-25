import logging
import logging.handlers
from pathlib import Path
import sys

_logger = None  # Instance du logger global


def init_logging(log_dir: Path):
    """
    Initialise le système de logs global.
    Appelée depuis main.py avec logs_dir = ~/.etl_multi_db/logs
    """

    global _logger

    log_file = log_dir / "etl_app_log"

    # Format des logs
    log_format = "%(asctime)s [%(levelname)s] %(name)s : %(message)s"
    formatter = logging.Formatter(log_format)

    # ============================================================
    #  HANDLER FICHIER — UTF-8 forcé
    # ============================================================
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5_000_000,       # 5 Mo
        backupCount=5,            # 5 fichiers max
        encoding="utf-8"          # 🔥 FIX : encode toujours en UTF-8
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # ============================================================
    #  HANDLER CONSOLE — UTF-8 sécurisé (Windows-friendly)
    # ============================================================
    try:
        # Windows ne gère pas bien l'UTF-8 → on force
        console_stream = open(sys.stdout.fileno(), "w", encoding="utf-8", closefd=False)
    except Exception:
        # Si l'ouverture par fileno échoue, on garde la sortie par défaut
        console_stream = sys.stdout

    console_handler = logging.StreamHandler(console_stream)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # ============================================================
    # LOGGER GLOBAL
    # ============================================================
    _logger = logging.getLogger("ETL_APP")
    _logger.setLevel(logging.INFO)

    # Évite la duplication des logs si le logger était déjà configuré
    if not _logger.hasHandlers():
        _logger.addHandler(file_handler)
        _logger.addHandler(console_handler)

    _logger.info("=== ETL Logs Initialized ===")
    _logger.info(f"Log file: {log_file}")


def get_logger(name: str = None) -> logging.Logger:
    """
    Retourne un logger.
    Si name est None → retourne le logger principal.
    Sinon → sous-logger nommé.
    """
    global _logger

    if _logger is None:
        raise RuntimeError("Logging not initialized. Call init_logging() first.")

    if name is None:
        return _logger

    return logging.getLogger(f"ETL_APP.{name}")
