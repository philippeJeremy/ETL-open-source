import logging
import logging.handlers
from pathlib import Path

_logger = None # Instance du logger global


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
    
    # Handler : rotation des fichiers (5 fichiers de 5 Mo)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5_000_000, # 5 Mo
        backupCount=5       # 5 fichiers max
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Handler console (optionnel)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Logger global
    _logger = logging.getLogger("ETL_APP")
    _logger.setLevel(logging.INFO)
    _logger.addHandler(file_handler)
    _logger.addHandler(console_handler)
    
    _logger.info("=== ETL Logs Initialized ===")
    _logger.info(f"Log file: {log_file}")
    
def get_logger(name: str=None) -> logging.Logger:
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