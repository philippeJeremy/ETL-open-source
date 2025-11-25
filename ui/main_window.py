# ui/main_window.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, List

from PySide6.QtCore import Qt, Slot, QTimer
from PySide6.QtWidgets import (
    QMainWindow, QTabWidget, QWidget,
    QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QPushButton, QLabel,
    QMessageBox, QPlainTextEdit, QAbstractItemView,
    QDialog, QFileDialog  
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

        # ---- TABWIDGET ----
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # ----------------------------------------------------
        #  ONGLET : TÂCHES
        # ----------------------------------------------------
        self.tab_tasks = QWidget()
        tasks_layout = QVBoxLayout(self.tab_tasks)

        # Titre
        title = QLabel("Tâches ETL planifiées")
        title.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        title.setStyleSheet("font-size: 20px; font-weight: bold;")
        tasks_layout.addWidget(title)

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
        tasks_layout.addWidget(self.table)

        # Boutons
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

        tasks_layout.addLayout(btn_layout)

        # Ajouter l'onglet
        self.tabs.addTab(self.tab_tasks, "Tâches")

        # ----------------------------------------------------
        #  ONGLET : LOGS
        # ----------------------------------------------------
        self.tab_logs = QWidget()
        self.logs_layout = QVBoxLayout(self.tab_logs)

        # Zone texte
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.logs_layout.addWidget(self.log_view)

        # Boutons logs
        log_btn_layout = QHBoxLayout()
        self.btn_refresh_logs = QPushButton("Rafraîchir")
        self.btn_clear_logs = QPushButton("Effacer affichage")
        self.btn_open_log = QPushButton("Ouvrir fichier log")

        log_btn_layout.addWidget(self.btn_refresh_logs)
        log_btn_layout.addWidget(self.btn_clear_logs)
        log_btn_layout.addWidget(self.btn_open_log)
        log_btn_layout.addStretch()

        self.logs_layout.addLayout(log_btn_layout)

        self.tabs.addTab(self.tab_logs, "Logs")

        # ----------------------------------------------------
        # CONNECT SIGNALS
        # ----------------------------------------------------
        self.btn_connections.clicked.connect(self.on_connections_clicked)
        self.btn_refresh.clicked.connect(self.on_refresh_clicked)
        self.btn_new.clicked.connect(self.on_new_clicked)
        self.btn_edit.clicked.connect(self.on_edit_clicked)
        self.btn_delete.clicked.connect(self.on_delete_clicked)
        self.btn_toggle.clicked.connect(self.on_toggle_clicked)
        self.btn_run.clicked.connect(self.on_run_clicked)

        self.btn_refresh_logs.clicked.connect(self.load_log_file)
        self.btn_clear_logs.clicked.connect(lambda: self.log_view.clear())
        self.btn_open_log.clicked.connect(self.open_log_file)

        # ----------------------------------------------------
        # Finalisation
        # ----------------------------------------------------
        self.setCentralWidget(central)
        
        # ----------------------------------------------------
        # Barre de statut
        # ----------------------------------------------------
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Prêt.")
        
        # # ---------------------------------------------
        # # ONGLET : Visualisation 3D
        # # ---------------------------------------------
        # self.tab_3d = QWidget()
        # tab3d_layout = QVBoxLayout(self.tab_3d)

        # self.db3d_view = Db3DView(self.repository, self)
        # tab3d_layout.addWidget(self.db3d_view)

        # self.tabs.addTab(self.tab_3d, "Visualisation 3D")

        # Auto refresh logs toutes les 3s
        self.log_file_path = str(Path.home() / ".etl_multi_db" / "logs" / "etl_app_log")
        self.log_timer = QTimer()
        self.log_timer.timeout.connect(self.load_log_file)
        self.log_timer.start(3000)

    # ------------------------------------------------------------------
    # Chargement / affichage des tâches
    # ------------------------------------------------------------------
    def _load_tasks(self):
        """Charge les tâches depuis le Repository"""
        self.tasks = self.repository.list_tasks(include_steps=True)
        self._refresh_table()
        self.status_bar.showMessage(f"{len(self.tasks)} tâche(s) chargée(s).")
        
    def load_log_file(self):
        try:
            # Essayer UTF-8
            try:
                with open(self.log_file_path, "r", encoding="utf-8") as f:
                    content = f.read()
            except UnicodeDecodeError:
                # Fallback CP1252 (Windows)
                with open(self.log_file_path, "r", encoding="cp1252", errors="replace") as f:
                    content = f.read()

            if self.log_view.toPlainText() != content:
                self.log_view.setPlainText(content)
                self.log_view.verticalScrollBar().setValue(
                    self.log_view.verticalScrollBar().maximum()
                )
        except Exception as e:
            self.log.error(f"Erreur lecture log : {e}")
        
    def open_log_file(self):
        try:
            QFileDialog.getOpenFileName(
                self,
                "Ouvrir log",
                self.log_file_path,
                "Log (*.log *.txt)"
            )
        except Exception as e:
            self.log.error(f"Erreur ouverture fichier log : {e}")

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
            self.log.warning("Tentative d'exécuter une tâche sans sélection")
            return

        # EXÉCUTION DIRECTE (sans popup)
        self.log.info(f"[MANUEL] Début exécution de la tâche : {task.name}")

        try:
            self.engine.run_task(task)
            self.log.info(f"[MANUEL] Tâche '{task.name}' exécutée avec succès")
        except Exception as e:
            self.log.error(
                f"[MANUEL] Erreur lors de l'exécution de '{task.name}' : {e}"
            )

    @Slot()
    def on_connections_clicked(self):
        dialog = ConnectionManagerDialog(self.repository, self)
        dialog.exec()
    
    @Slot()
    def on_db3d_clicked(self):
        from ui.db_3d_view import Db3DSelectorDialog

        dialog = Db3DSelectorDialog(self.repository, parent=self)
        dialog.exec()
