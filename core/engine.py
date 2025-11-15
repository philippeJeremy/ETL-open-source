#♀ core/engine
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from services.log_service import get_logger
from core.models import ScheduledTask, Step, StepType
from storage.repository import Repository


class EtlEngine:
    """
    Moteur ETL principal.
    Gère l'éxecution complète d'une tache :
        - extraction
        - transformations
        - chargement (load)
    """

    def __init__(self, repository: Repository):
        self.repository = repository
        self.log = get_logger("EtlEngine")

    # ---------------------------------------------------------
    # Public : exécuter une tâche complète (par id)
    # ---------------------------------------------------------
    def run_task_by_id(self, task_id: int) -> None:
        task = self.repository.get_task(task_id, include_steps=True)
        self.run_task(task)

    # ---------------------------------------------------------
    # Public : exécuter un ScheduledTask
    # ---------------------------------------------------------
    def run_task(self, task: ScheduledTask) -> None:
        self.log.info(f"===== DÉBUT EXÉCUTION : {task.name} =====")
        start_time = datetime.now().isoformat()

        # Historique : démarrage
        history_id = self.repository.log_execution_start(
            task_id=task.id,
            started_at=start_time
        )

        try:
            data = None  # le DataFrame / liste de dicts circule de step en step

            # Exécution step par step
            for step in sorted(task.steps, key=lambda s: s.order):
                self.log.info(
                    f"--- Step {step.order} : {step.name} ({step.step_type}) ---")
                data = self._execute_step(step, data)

            # Historique : succès
            end_time = datetime.now().isoformat()
            self.repository.log_execution_end(
                history_id,
                finished_at=end_time,
                status="success",
                message=None
            )

            self.log.info(f"===== FIN OK : {task.name} =====")

        except Exception as e:
            # Historique : erreur
            end_time = datetime.now().isoformat()
            self.repository.log_execution_end(
                history_id,
                finished_at=end_time,
                status="error",
                message=str(e)
            )

            self.log.error(f"ERREUR dans la tâche {task.name} : {e}")
            raise

    # ---------------------------------------------------------
    # Exécuter un step
    # ---------------------------------------------------------
    def _execute_step(self, step: Step, data_in: Any) -> Any:
        """
        Retourne data_out
        """
        if step.step_type == StepType.EXTRACT:
            return self._run_extract(step)

        elif step.step_type == StepType.TRANSFORM:
            return self._run_transform(step, data_in)

        elif step.step_type == StepType.LOAD:
            self._run_load(step, data_in)
            return data_in

        else:
            raise ValueError(f"Type de step inconnu : {step.step_type}")

    # ---------------------------------------------------------
    # EXTRACT
    # ---------------------------------------------------------

    def _run_extract(self, step: Step):
        conn = self.repository.get_connection_by_id(step.connection_id)

        if conn.type == "sqlserver":
            from core.extractors.sqlserver import SqlServerExtractor
            extractor = SqlServerExtractor(conn.params, step.config)
            return extractor.extract()
        
        # if conn.type == "postgres":
        #     from core.extractors.postgres import PostgresExtractor
        #     extractor = PostgresExtractor(conn.params, step.config)
        #     return extractor.extract()

        # elif conn.type == "mysql":
        #     from core.extractors.mysql import MysqlExtractor
        #     extractor = MysqlExtractor(conn.params, step.config)
        #     return extractor.extract()

        # elif conn.type == "csv":
        #     from core.extractors.csv_extractor import CsvExtractor
        #     extractor = CsvExtractor(step.config)
        #     return extractor.extract()
        

        
        # elif conn.type == "oracle":
        #     from core.extractors.oracle import OracleExtractor
        #     extractor = CsvExtractor(step.config)
        #     return extractor.extract()

        # elif conn.type == "mongo":
        #     from core.extractors.oracle import MongoExtractor
        #     extractor = CsvExtractor(step.config)
        #     return extractor.extract()

        else:
            raise NotImplementedError(
                f"Extracteur non implémenté pour type={conn.type}")

    # ---------------------------------------------------------
    # TRANSFORM
    # ---------------------------------------------------------
    def _run_transform(self, step: Step, data):
        if data is None:
            raise ValueError("Impossible d'appliquer un transform : data=None")

        trans_type = step.config.get("type")

        if trans_type == "pandas":
            from core.transformers.pandas_transformer import PandasTransformer
            transformer = PandasTransformer(step.config)
            return transformer.transform(data)

        elif trans_type == "python":
            from core.transformers.custom_transformer import PythonTransformer
            transformer = PythonTransformer(step.config)
            return transformer.transform(data)

        else:
            raise NotImplementedError(
                f"Transform non implémenté : {trans_type}")
    
    # ---------------------------------------------------------
    # LOAD
    # ---------------------------------------------------------
    def _run_load(self, step: Step, data):
        conn = self.repository.get_connection_by_id(step.connection_id)

        if conn.type == "sqlserver":
            from core.loaders.sqlserver import SqlServerLoader
            loader = SqlServerLoader(conn.params, step.config)
            return loader.load(data)
        
        # elif conn.type == "postgres":
        #     from core.loaders.postgres import PostgresLoader
        #     loader = PostgresLoader(conn.params, step.config)
        #     return loader.load(data)

        # elif conn.type == "mysql":
        #     from core.loaders.mysql import MysqlLoader
        #     loader = MysqlLoader(conn.params, step.config)
        #     return loader.load(data)

        # elif conn.type == "csv":
        #     from core.loaders.csv_loader import CsvLoader
        #     loader = CsvLoader(step.config)
        #     return loader.load(data)

        else:
            raise NotImplementedError(f"Loader non implémenté pour type={conn.type}")
