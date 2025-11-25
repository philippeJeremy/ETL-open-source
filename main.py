import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from ui.main_window import MainWindow
from storage.repository import Repository
from core.engine import EtlEngine
from scheduler.scheduler import Scheduler
from services.log_service import init_logging, get_logger


def main():
    # Application Qt
    app = QApplication(sys.argv)
    app.setApplicationName("Multi-DB ETL Planner")

    # Répertoire de travail
    base_dir = Path.home() / ".etl_multi_db"
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Initialisation logs
    init_logging(logs_dir)
    log = get_logger("Main")
    log.info("Application ETL démarrée")

    # Repository SQLite
    repository = Repository(base_dir=base_dir)

    # Moteur ETL
    engine = EtlEngine(repository=repository)

    # Scheduler
    scheduler = Scheduler(repository=repository, engine=engine)
    scheduler.start()

    # Fenêtre principale
    window = MainWindow(
        repository=repository,
        engine=engine,
        scheduler=scheduler
    )
    window.show()

    # boucle Qt
    exit_code = app.exec()

    # Arrêt propre
    scheduler.stop()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
