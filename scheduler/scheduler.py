# scheduler/scheduler.py
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Dict

from services.log_service import get_logger
from scheduler.cron_parser import next_run_from_cron
from scheduler.worker import TaskWorker


class Scheduler(threading.Thread):
    """
    Planificateur simple :
        - lit les tâches activées depuis le Repository
        - calcule la prochaine exécution à partir du cron
        - lance les exécutions quand nécessaire
        - tourne en arrière-plan
    """

    def __init__(self, repository, engine, check_interval_seconds=30):
        super().__init__(daemon=True)
        self.repository = repository
        self.engine = engine
        self.check_interval_seconds = check_interval_seconds

        self.log = get_logger("Scheduler")
        self._running = threading.Event()
        self._running.set()

        # Cache : {task_id: datetime prochaine_exécution}
        self.next_runs: Dict[int, datetime] = {}

    # ------------------------------------------------------
    # Contrôle du scheduler
    # ------------------------------------------------------
    def stop(self):
        self.log.info("Arrêt du scheduler...")
        self._running.clear()

    # ------------------------------------------------------
    # Thread principal
    # ------------------------------------------------------
    def run(self):
        self.log.info("Scheduler démarré.")

        while self._running.is_set():
            try:
                self._check_and_run_tasks()
            except Exception as e:
                self.log.error(f"Erreur dans le scheduler : {e}")

            time.sleep(self.check_interval_seconds)

        self.log.info("Scheduler arrêté.")

    # ------------------------------------------------------
    # Vérifier toutes les tâches
    # ------------------------------------------------------
    def _check_and_run_tasks(self):
        tasks = self.repository.list_enabled_tasks()
        now = datetime.now()

        for task in tasks:
            if task.id not in self.next_runs:
                # Première programmation
                next_time = next_run_from_cron(task.recurrence, now)
                self.next_runs[task.id] = next_time
                self.log.info(f"Tâche '{task.name}' première planification : {next_time}")
                continue

            # Vérifier si la tâche doit s'exécuter
            if now >= self.next_runs[task.id]:
                self._execute_task(task)

                # Replanifier la prochaine exécution
                next_time = next_run_from_cron(task.recurrence, now)
                self.next_runs[task.id] = next_time

    # ------------------------------------------------------
    # Exécuter une tâche dans un worker thread
    # ------------------------------------------------------
    def _execute_task(self, task):
        self.log.info(f"Exécution planifiée de la tâche : {task.name}")
        worker = TaskWorker(self.engine, task)
        worker.start()
