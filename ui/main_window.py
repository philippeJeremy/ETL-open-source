# ui/main_window.py
from __future__ import annotations

from typing import Optional, List

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QLabel,
    QMessageBox,
    QStatusBar,
    QAbstractItemView,
    QDialog,
)

from services.log_service import get_logger
from core.models import ScheduledTask
from storage.repository import Repository
from core.engine import EtlEngine
from scheduler.scheduler import Scheduler
from ui.connection_manager import ConnectionManagerDialog
from ui.task_editor import TaskEditorDialog


class MainWindow(QMainWindow):
    """
    Fenêtre principale de l'application ETL.

    Affiche la liste des tâches planifiées,
    permet de les exécuter, activer/désactiver, ajouter/supprimer.
    """

    def __init__(
        self,
        repository: Repository,
        engine: EtlEngine,
        scheduler: Scheduler,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)

        self.repository = repository
        self.engine = engine
        self.scheduler = scheduler

        self.log = get_logger("MainWindow")

        self.tasks: List[ScheduledTask] = []

        self.setWindowTitle("Multi-DB ETL Planner")
        self.resize(1000, 600)

        self._init_ui()
        self._load_tasks()

    # ------------------------------------------------------------------
    # Construction de l'UI
    # ------------------------------------------------------------------
    def _init_ui(self):
        central = QWidget(self)
        main_layout = QVBoxLayout(central)

        # Titre
        title = QLabel("Tâches ETL planifiées")
        title.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        title.setStyleSheet("font-size: 20px; font-weight: bold;")
        main_layout.addWidget(title)

        # Tableau des tâches
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(
            ["Nom", "Récurrence", "Étapes", "Activée ?"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)

        main_layout.addWidget(self.table)

        # Boutons d'action
        btn_layout = QHBoxLayout()

        self.btn_refresh = QPushButton("Rafraîchir")
        self.btn_new = QPushButton("Nouvelle tâche")
        self.btn_edit = QPushButton("Modifier")
        self.btn_delete = QPushButton("Supprimer")
        self.btn_toggle = QPushButton("Activer / Désactiver")
        self.btn_run = QPushButton("Exécuter maintenant")
        self.btn_connections = QPushButton("Connexions...")

        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addWidget(self.btn_new)
        btn_layout.addWidget(self.btn_edit)
        btn_layout.addWidget(self.btn_delete)
        btn_layout.addWidget(self.btn_toggle)
        btn_layout.addWidget(self.btn_run)
        btn_layout.addWidget(self.btn_connections)

        main_layout.addLayout(btn_layout)
        self.btn_connections.clicked.connect(self.on_connections_clicked)
        self.setCentralWidget(central)

        # Barre de statut
        status = QStatusBar()
        self.setStatusBar(status)
        self.status_bar = status

        # Connexion des signaux
        self.btn_refresh.clicked.connect(self.on_refresh_clicked)
        self.btn_new.clicked.connect(self.on_new_clicked)
        self.btn_edit.clicked.connect(self.on_edit_clicked)
        self.btn_delete.clicked.connect(self.on_delete_clicked)
        self.btn_toggle.clicked.connect(self.on_toggle_clicked)
        self.btn_run.clicked.connect(self.on_run_clicked)

    # ------------------------------------------------------------------
    # Chargement / affichage des tâches
    # ------------------------------------------------------------------
    def _load_tasks(self):
        """Charge les tâches depuis le Repository"""
        self.tasks = self.repository.list_tasks(include_steps=True)
        self._refresh_table()
        self.status_bar.showMessage(f"{len(self.tasks)} tâche(s) chargée(s).")

    def _refresh_table(self):
        self.table.setRowCount(len(self.tasks))

        for row, task in enumerate(self.tasks):
            item_name = QTableWidgetItem(task.name)
            item_recur = QTableWidgetItem(task.recurrence)
            item_steps = QTableWidgetItem(str(len(task.steps)))
            item_enabled = QTableWidgetItem("Oui" if task.enabled else "Non")

            # Alignements
            item_steps.setTextAlignment(Qt.AlignCenter)
            item_enabled.setTextAlignment(Qt.AlignCenter)

            self.table.setItem(row, 0, item_name)
            self.table.setItem(row, 1, item_recur)
            self.table.setItem(row, 2, item_steps)
            self.table.setItem(row, 3, item_enabled)

        self.table.resizeColumnsToContents()

    def _get_selected_index(self) -> Optional[int]:
        selection = self.table.selectionModel().selectedRows()
        if not selection:
            return None
        return selection[0].row()

    def _get_selected_task(self) -> Optional[ScheduledTask]:
        idx = self._get_selected_index()
        if idx is None or idx < 0 or idx >= len(self.tasks):
            return None
        return self.tasks[idx]

    # ------------------------------------------------------------------
    # Slots des boutons
    # ------------------------------------------------------------------
    @Slot()
    def on_refresh_clicked(self):
        self.log.info("Rafraîchissement des tâches depuis la base")
        self._load_tasks()

    @Slot()
    def on_new_clicked(self):
        dialog = TaskEditorDialog(self.repository, existing=None, parent=self)
        if dialog.exec() == QDialog.Accepted:
            try:
                task = dialog.build_task(existing_id=None)
                self.repository.save_task(task)
                self._load_tasks()
            except Exception as e:
                QMessageBox.critical(
                    self, "Erreur", f"Impossible d'enregistrer la tâche :\n{e}")

    @Slot()
    def on_edit_clicked(self):
        task = self._get_selected_task()
        if not task:
            QMessageBox.warning(self, "Aucune sélection",
                                "Sélectionne une tâche.")
            return

        dialog = TaskEditorDialog(self.repository, existing=task, parent=self)
        if dialog.exec() == QDialog.Accepted:
            try:
                new_task = dialog.build_task(existing_id=task.id)
                self.repository.save_task(new_task)
                self._load_tasks()
            except Exception as e:
                QMessageBox.critical(
                    self, "Erreur", f"Impossible de mettre à jour la tâche :\n{e}")

    @Slot()
    def on_delete_clicked(self):
        task = self._get_selected_task()
        if not task:
            QMessageBox.warning(self, "Aucune sélection",
                                "Sélectionne une tâche.")
            return

        reply = QMessageBox.question(
            self,
            "Confirmer la suppression",
            f"Supprimer la tâche '{task.name}' ?",
        )

        if reply == QMessageBox.Yes:
            try:
                self.repository.delete_task(task.id)
                self.log.info(f"Tâche supprimée : {task.name}")
                self._load_tasks()
            except Exception as e:
                QMessageBox.critical(
                    self, "Erreur", f"Impossible de supprimer : {e}")

    @Slot()
    def on_toggle_clicked(self):
        task = self._get_selected_task()
        if not task:
            QMessageBox.warning(self, "Aucune sélection",
                                "Sélectionne une tâche.")
            return

        task.enabled = not task.enabled

        try:
            self.repository.save_task(task)
            state = "activée" if task.enabled else "désactivée"
            self.log.info(f"Tâche {state} : {task.name}")
            self._load_tasks()
        except Exception as e:
            QMessageBox.critical(
                self, "Erreur", f"Impossible de mettre à jour : {e}")

    @Slot()
    def on_run_clicked(self):
        task = self._get_selected_task()
        if not task:
            QMessageBox.warning(self, "Aucune sélection",
                                "Sélectionne une tâche.")
            return

        # Lancement manuel → blocant pour l'instant (simple).
        # Plus tard : on pourra le lancer dans un thread (TaskWorker).
        reply = QMessageBox.question(
            self,
            "Exécution manuelle",
            f"Exécuter maintenant la tâche '{task.name}' ?",
        )

        if reply != QMessageBox.Yes:
            return

        try:
            self.log.info(f"Exécution manuelle de la tâche : {task.name}")
            self.engine.run_task(task)
            QMessageBox.information(
                self,
                "Exécution terminée",
                f"La tâche '{task.name}' s'est terminée avec succès.",
            )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Erreur d'exécution",
                f"Erreur lors de l'exécution de la tâche '{task.name}' :\n{e}",
            )

    @Slot()
    def on_connections_clicked(self):
        dialog = ConnectionManagerDialog(self.repository, self)
        dialog.exec()
