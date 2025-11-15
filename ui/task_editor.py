# ui/task_editor.py
from __future__ import annotations

from typing import Optional, List

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QDialog, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLabel, QLineEdit, QCheckBox, QPushButton, QTableWidget,
    QTableWidgetItem, QMessageBox, QAbstractItemView
)

from core.models import ScheduledTask, Step
from storage.repository import Repository
from services.log_service import get_logger
from ui.step_editor import StepEditorDialog


class TaskEditorDialog(QDialog):
    """
    Éditeur de tâche ETL (ScheduledTask) :
    - nom
    - récurrence (cron)
    - activée
    - liste des steps (Extract / Load / …)
    """

    def __init__(
        self,
        repository: Repository,
        existing: Optional[ScheduledTask] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.repository = repository
        self.existing = existing
        self.log = get_logger("TaskEditorDialog")

        self.setWindowTitle("Éditeur de tâche ETL")
        self.resize(800, 600)

        # copie locale des steps (on ne touche pas directement à existing.steps)
        self.steps: List[Step] = []
        if existing:
            # cloner les steps (en gardant id, config, etc.)
            self.steps = [Step(
                id=s.id,
                task_id=s.task_id,
                name=s.name,
                step_type=s.step_type,
                order=s.order,
                connection_id=s.connection_id,
                config=dict(s.config),
            ) for s in existing.steps]

        self._init_ui()

        if existing:
            self._load_existing(existing)

    # ------------------------------------------------------------------
    def _init_ui(self):
        layout = QVBoxLayout(self)

        # Infos générales
        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.recur_edit = QLineEdit()
        self.recur_edit.setPlaceholderText("0 * * * *  (toutes les heures par ex.)")

        self.enabled_chk = QCheckBox("Tâche activée")

        form.addRow("Nom de la tâche :", self.name_edit)
        form.addRow("Récurrence (CRON) :", self.recur_edit)
        form.addRow("", self.enabled_chk)

        layout.addLayout(form)

        # Label / séparation
        layout.addWidget(QLabel("Étapes du pipeline :"))

        # Tableau des steps
        self.steps_table = QTableWidget()
        self.steps_table.setColumnCount(4)
        self.steps_table.setHorizontalHeaderLabels(["Ordre", "Nom", "Type", "Connexion"])
        self.steps_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.steps_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.steps_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.steps_table.horizontalHeader().setStretchLastSection(True)

        layout.addWidget(self.steps_table)

        # Boutons steps
        btn_steps = QHBoxLayout()
        self.btn_add_step = QPushButton("Ajouter étape")
        self.btn_edit_step = QPushButton("Modifier étape")
        self.btn_delete_step = QPushButton("Supprimer étape")
        self.btn_up_step = QPushButton("Monter")
        self.btn_down_step = QPushButton("Descendre")

        btn_steps.addWidget(self.btn_add_step)
        btn_steps.addWidget(self.btn_edit_step)
        btn_steps.addWidget(self.btn_delete_step)
        btn_steps.addStretch()
        btn_steps.addWidget(self.btn_up_step)
        btn_steps.addWidget(self.btn_down_step)

        layout.addLayout(btn_steps)

        # Boutons bas
        btn_bottom = QHBoxLayout()
        self.btn_ok = QPushButton("Enregistrer")
        self.btn_cancel = QPushButton("Annuler")

        btn_bottom.addStretch()
        btn_bottom.addWidget(self.btn_ok)
        btn_bottom.addWidget(self.btn_cancel)

        layout.addLayout(btn_bottom)

        # Signaux
        self.btn_add_step.clicked.connect(self.on_add_step_clicked)
        self.btn_edit_step.clicked.connect(self.on_edit_step_clicked)
        self.btn_delete_step.clicked.connect(self.on_delete_step_clicked)
        self.btn_up_step.clicked.connect(self.on_up_step_clicked)
        self.btn_down_step.clicked.connect(self.on_down_step_clicked)

        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

        # Initial refresh
        self._refresh_steps_table()

    # ------------------------------------------------------------------
    def _load_existing(self, task: ScheduledTask):
        self.name_edit.setText(task.name)
        self.recur_edit.setText(task.recurrence)
        self.enabled_chk.setChecked(task.enabled)

    def _refresh_steps_table(self):
        # Réordonner les steps par order
        self.steps.sort(key=lambda s: s.order)

        self.steps_table.setRowCount(len(self.steps))

        # Pour afficher le nom de la connexion, on va chercher dans le repository
        connections = {c.id: c for c in self.repository.list_connections()}

        for row, step in enumerate(self.steps):
            order_item = QTableWidgetItem(str(step.order))
            name_item = QTableWidgetItem(step.name)
            type_item = QTableWidgetItem(step.step_type.value)

            conn_name = ""
            if step.connection_id is not None and step.connection_id in connections:
                conn_name = connections[step.connection_id].name

            conn_item = QTableWidgetItem(conn_name)

            order_item.setTextAlignment(Qt.AlignCenter)
            type_item.setTextAlignment(Qt.AlignCenter)

            self.steps_table.setItem(row, 0, order_item)
            self.steps_table.setItem(row, 1, name_item)
            self.steps_table.setItem(row, 2, type_item)
            self.steps_table.setItem(row, 3, conn_item)

        self.steps_table.resizeColumnsToContents()

    def _get_selected_step_index(self) -> Optional[int]:
        sel = self.steps_table.selectionModel().selectedRows()
        if not sel:
            return None
        return sel[0].row()

    # ------------------------------------------------------------------
    # Slots Steps
    # ------------------------------------------------------------------
    @Slot()
    def on_add_step_clicked(self):
        dialog = StepEditorDialog(self.repository, existing=None, parent=self)
        if dialog.exec() == QDialog.Accepted:
            # ordre = dernier + 1
            next_order = len(self.steps) + 1
            try:
                step = dialog.build_step(base_order=next_order)
            except Exception as e:
                QMessageBox.critical(self, "Erreur", str(e))
                return
            print(type(step.step_type), step.step_type)
            self.steps.append(step)
            self._refresh_steps_table()

    @Slot()
    def on_edit_step_clicked(self):
        idx = self._get_selected_step_index()
        if idx is None:
            QMessageBox.warning(self, "Aucune sélection", "Sélectionne une étape.")
            return

        current_step = self.steps[idx]
        dialog = StepEditorDialog(self.repository, existing=current_step, parent=self)
        if dialog.exec() == QDialog.Accepted:
            try:
                new_step = dialog.build_step(
                    base_order=current_step.order,
                    existing_id=current_step.id,
                    task_id=current_step.task_id,
                )
            except Exception as e:
                QMessageBox.critical(self, "Erreur", str(e))
                return

            self.steps[idx] = new_step
            self._refresh_steps_table()

    @Slot()
    def on_delete_step_clicked(self):
        idx = self._get_selected_step_index()
        if idx is None:
            QMessageBox.warning(self, "Aucune sélection", "Sélectionne une étape.")
            return

        del self.steps[idx]

        # Recalculer les order
        for i, step in enumerate(self.steps, start=1):
            step.order = i

        self._refresh_steps_table()

    @Slot()
    def on_up_step_clicked(self):
        idx = self._get_selected_step_index()
        if idx is None or idx <= 0:
            return

        self.steps[idx - 1], self.steps[idx] = self.steps[idx], self.steps[idx - 1]

        # Reorder
        for i, step in enumerate(self.steps, start=1):
            step.order = i

        self._refresh_steps_table()
        self.steps_table.selectRow(idx - 1)

    @Slot()
    def on_down_step_clicked(self):
        idx = self._get_selected_step_index()
        if idx is None or idx >= len(self.steps) - 1:
            return

        self.steps[idx + 1], self.steps[idx] = self.steps[idx], self.steps[idx + 1]

        # Reorder
        for i, step in enumerate(self.steps, start=1):
            step.order = i

        self._refresh_steps_table()
        self.steps_table.selectRow(idx + 1)

    # ------------------------------------------------------------------
    # Récupérer la ScheduledTask complétée
    # ------------------------------------------------------------------
    def build_task(self, existing_id: Optional[int] = None) -> ScheduledTask:
        name = self.name_edit.text().strip()
        recur = self.recur_edit.text().strip()
        enabled = self.enabled_chk.isChecked()

        if not name:
            raise ValueError("Le nom de la tâche est obligatoire.")
        if not recur:
            raise ValueError("La récurrence est obligatoire.")

        # Re-ordonner les steps par order
        self.steps.sort(key=lambda s: s.order)

        return ScheduledTask(
            id=existing_id,
            name=name,
            recurrence=recur,
            enabled=enabled,
            steps=self.steps,
        )
