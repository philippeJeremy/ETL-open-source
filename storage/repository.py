from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional

from core.models import ConnectionConfig, ScheduledTask, Step, StepType
from services.log_service import get_logger


class Repository:
    """
    Repository central basé sur SQLite.
    Gère :
        - les connexions (ConnectionConfig)
        - les tâches (ScheduledTask)
        - les steps (Step)
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.db_path = self.base_dir / "etl.db"
        self._log = get_logger("Repository")

        # check_same_thread=False pour pouvoir utiliser la même connexion
        # depuis un thread scheduler + le thread UI (attention à rester simple au début).
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        self._init_schema()

    # ------------------------------------------------------------------
    # Initialisation du schéma
    # ------------------------------------------------------------------
    def _init_schema(self):
        self._log.info(f"Initialisation de la base SQLite : {self.db_path}")

        cur = self._conn.cursor()

        # Connexions
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                params TEXT NOT NULL
            )
            """
        )

        # Tâches
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                recurrence TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )

        # Steps
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                step_type TEXT NOT NULL,
                step_order INTEGER NOT NULL,
                connection_id INTEGER,
                config TEXT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (connection_id) REFERENCES connections(id)
            )
            """
        )

        # Historique d'exécution (optionnel pour plus tard)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,     -- "running", "success", "error"
                message TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            )
            """
        )

        self._conn.commit()
        
    # ------------------------------------------------------------------
    # Connexions (ConnectionConfig)
    # ------------------------------------------------------------------
    def list_connections(self) -> List[ConnectionConfig]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM connections ORDER BY name")
        rows = cur.fetchall()
        return [
            ConnectionConfig(
                id=row["id"],
                name=row["name"],
                type=row["type"],
                params=json.loads(row["params"]),
            )
            for row in rows
        ]

    def get_connection_by_id(self, conn_id: int) -> ConnectionConfig:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM connections WHERE id = ?", (conn_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Connexion id={conn_id} introuvable")

        return ConnectionConfig(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            params=json.loads(row["params"]),
        )

    def get_connection_by_name(self, name: str) -> ConnectionConfig:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM connections WHERE name = ?", (name,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Connexion '{name}' introuvable")

        return ConnectionConfig(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            params=json.loads(row["params"]),
        )

    def save_connection(self, conn_cfg: ConnectionConfig) -> ConnectionConfig:
        """
        Insert ou update une connexion.
        Si conn_cfg.id est None -> INSERT
        Sinon -> UPDATE
        """
        cur = self._conn.cursor()
        params_json = json.dumps(conn_cfg.params)

        if conn_cfg.id is None:
            cur.execute(
                "INSERT INTO connections (name, type, params) VALUES (?, ?, ?)",
                (conn_cfg.name, conn_cfg.type, params_json),
            )
            conn_cfg.id = cur.lastrowid
        else:
            cur.execute(
                """
                UPDATE connections
                SET name = ?, type = ?, params = ?
                WHERE id = ?
                """,
                (conn_cfg.name, conn_cfg.type, params_json, conn_cfg.id),
            )

        self._conn.commit()
        self._log.info(
            f"Connexion sauvegardée : {conn_cfg.name} (id={conn_cfg.id})")
        return conn_cfg

    def delete_connection(self, conn_id: int) -> None:
        cur = self._conn.cursor()
        cur.execute("DELETE FROM connections WHERE id = ?", (conn_id,))
        self._conn.commit()
        self._log.info(f"Connexion supprimée id={conn_id}")

    def list_tasks(self, include_steps: bool = True) -> List[ScheduledTask]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM tasks ORDER BY name")
        task_rows = cur.fetchall()

        tasks: List[ScheduledTask] = []
        for row in task_rows:
            task = ScheduledTask(
                id=row["id"],
                name=row["name"],
                recurrence=row["recurrence"],
                enabled=bool(row["enabled"]),
                steps=[],
            )
            if include_steps:
                task.steps = self._load_steps_for_task(task.id)
            tasks.append(task)

        return tasks

    def list_enabled_tasks(self) -> List[ScheduledTask]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE enabled = 1 ORDER BY name")
        task_rows = cur.fetchall()

        tasks: List[ScheduledTask] = []
        for row in task_rows:
            task = ScheduledTask(
                id=row["id"],
                name=row["name"],
                recurrence=row["recurrence"],
                enabled=True,
                steps=self._load_steps_for_task(row["id"]),
            )
            tasks.append(task)

        return tasks
    
    def get_task(self, task_id: int, include_steps: bool = True) -> ScheduledTask:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Tâche id={task_id} introuvable")

        task = ScheduledTask(
            id=row["id"],
            name=row["name"],
            recurrence=row["recurrence"],
            enabled=bool(row["enabled"]),
            steps=[],
        )
        if include_steps:
            task.steps = self._load_steps_for_task(task.id)
        return task
    
    def save_task(self, task: ScheduledTask) -> ScheduledTask:
        """
        Insert ou update une tâche + ses steps.
        Stratégie simple :
          - si nouvelle tâche -> INSERT task, puis INSERT steps
          - si update -> UPDATE task, DELETE steps existants, INSERT steps
        """
        cur = self._conn.cursor()

        if task.id is None:
            cur.execute(
                "INSERT INTO tasks (name, recurrence, enabled) VALUES (?, ?, ?)",
                (task.name, task.recurrence, int(task.enabled)),
            )
            task.id = cur.lastrowid
        else:
            cur.execute(
                """
                UPDATE tasks
                SET name = ?, recurrence = ?, enabled = ?
                WHERE id = ?
                """,
                (task.name, task.recurrence, int(task.enabled), task.id),
            )
            # On supprime les steps existants avant de réinsérer
            cur.execute("DELETE FROM steps WHERE task_id = ?", (task.id,))

        # (Ré)insertion des steps
        for step in sorted(task.steps, key=lambda s: s.order):
            self._insert_step(task.id, step)

        self._conn.commit()
        self._log.info(f"Tâche sauvegardée : {task.name} (id={task.id})")
        return task
    
    def delete_task(self, task_id: int) -> None:
        cur = self._conn.cursor()
        # steps supprimés automatiquement via ON DELETE CASCADE si FK bien gérée,
        # mais on peut assurer manuellement pour être sûr.
        cur.execute("DELETE FROM steps WHERE task_id = ?", (task_id,))
        cur.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        self._conn.commit()
        self._log.info(f"Tâche supprimée id={task_id}")
        
    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------
    def _load_steps_for_task(self, task_id: int) -> List[Step]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT * FROM steps
            WHERE task_id = ?
            ORDER BY step_order ASC
            """,
            (task_id,),
        )
        rows = cur.fetchall()
        steps: List[Step] = []
        for row in rows:
            steps.append(
                Step(
                    id=row["id"],
                    task_id=row["task_id"],
                    name=row["name"],
                    step_type=StepType(row["step_type"]),
                    order=row["step_order"],
                    connection_id=row["connection_id"],
                    config=json.loads(row["config"]),
                )
            )
        return steps
    
    def _insert_step(self, task_id: int, step: Step) -> Step:
        cur = self._conn.cursor()
        config_json = json.dumps(step.config)
        connection_id = step.connection_id

        cur.execute(
            """
            INSERT INTO steps (task_id, name, step_type, step_order, connection_id, config)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                step.name,
                step.step_type.value,
                step.order,
                connection_id,
                config_json,
            ),
        )
        step.id = cur.lastrowid
        step.task_id = task_id
        return step
    
    # ------------------------------------------------------------------
    # Historique d'exécution (API simple pour plus tard)
    # ------------------------------------------------------------------
    def log_execution_start(self, task_id: int, started_at: str) -> int:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO execution_history (task_id, started_at, status)
            VALUES (?, ?, ?)
            """,
            (task_id, started_at, "running"),
        )
        self._conn.commit()
        return cur.lastrowid

    def log_execution_end(
        self,
        history_id: int,
        finished_at: str,
        status: str,
        message: str | None = None,
    ) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            UPDATE execution_history
            SET finished_at = ?, status = ?, message = ?
            WHERE id = ?
            """,
            (finished_at, status, message, history_id),
        )
        self._conn.commit()