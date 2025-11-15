# scheduler/worker.py
from __future__ import annotations

import threading
from services.log_service import get_logger


class TaskWorker(threading.Thread):
    """
    Thread dédié à l'exécution d'une tâche unique.
    """

    def __init__(self, engine, task):
        super().__init__(daemon=True)
        self.engine = engine
        self.task = task
        self.log = get_logger("TaskWorker")

    def run(self):
        self.log.info(f"Démarrage du worker pour tâche : {self.task.name}")
        try:
            self.engine.run_task(self.task)
        except Exception as e:
            self.log.error(f"Erreur worker tâche '{self.task.name}': {e}")
        self.log.info(f"Worker terminé pour tâche : {self.task.name}")
