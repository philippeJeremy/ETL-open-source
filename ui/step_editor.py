# ui/step_editor.py
from __future__ import annotations

from typing import Optional, List

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QDialog, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLabel, QLineEdit, QComboBox, QPlainTextEdit, QPushButton,
    QCheckBox, QMessageBox, QAbstractItemView, QListWidget, QListWidgetItem
)

from core.models import Step, StepType, ConnectionConfig
from storage.repository import Repository
from services.log_service import get_logger
from core.extractors.sqlserver import SqlServerExtractor


class StepEditorDialog(QDialog):
    """
    Éditeur d'une étape ETL (Step).
    Pour l'instant : type = EXTRACT ou LOAD, sur SQL Server.
    """

    def __init__(
        self,
        repository: Repository,
        existing: Optional[Step] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.repository = repository
        self.existing = existing
        self.log = get_logger("StepEditorDialog")

        self.setWindowTitle("Éditeur d'étape ETL")
        self.resize(600, 500)

        self.connections: List[ConnectionConfig] = self.repository.list_connections()

        self._init_ui()

        if existing:
            self._load_existing(existing)

    # ------------------------------------------------------------------
    def _init_ui(self):
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.type_combo = QComboBox()
        self.type_combo.addItem("Extract", StepType.EXTRACT)
        # plus tard on pourra rajouter :
        # self.type_combo.addItem("Transform", StepType.TRANSFORM)
        self.type_combo.addItem("Load", StepType.LOAD)

        # Connexion (source pour EXTRACT, destination pour LOAD)
        self.conn_combo = QComboBox()
        for conn in self.connections:
            self.conn_combo.addItem(f"{conn.name} ({conn.type})", conn.id)

        form.addRow("Nom de l'étape :", self.name_edit)
        form.addRow("Type :", self.type_combo)
        form.addRow("Connexion :", self.conn_combo)

        layout.addLayout(form)

        # Zone de configuration spécifique selon type
        self.config_area = QVBoxLayout()
        layout.addLayout(self.config_area)

        # Widgets pour EXTRACT
        self.extract_widget = QWidget()
        extract_layout = QVBoxLayout(self.extract_widget)

        self.query_edit = QPlainTextEdit()
        self.query_edit.setPlaceholderText("SELECT * FROM dbo.MaTable;")

        # Liste des tables + bouton charger
        tables_layout = QHBoxLayout()
        self.btn_load_tables = QPushButton("Lister les tables")
        self.tables_list = QListWidget()
        self.tables_list.setSelectionMode(QAbstractItemView.SingleSelection)

        tables_layout.addWidget(self.btn_load_tables)
        extract_layout.addLayout(tables_layout)
        extract_layout.addWidget(QLabel("Requête SQL :"))
        extract_layout.addWidget(self.query_edit)
        extract_layout.addWidget(QLabel("Tables détectées (double-clic pour insérer un SELECT) :"))
        extract_layout.addWidget(self.tables_list)

        # Widgets pour LOAD
        self.load_widget = QWidget()
        load_layout = QFormLayout(self.load_widget)

        self.table_edit = QLineEdit()
        self.table_edit.setPlaceholderText("dbo.MaTableCible")

        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Append (ajouter)", "append")
        self.mode_combo.addItem("Replace (effacer puis insérer)", "replace")

        self.create_table_chk = QCheckBox("Créer la table si elle n'existe pas")

        load_layout.addRow("Table de destination :", self.table_edit)
        load_layout.addRow("Mode de chargement :", self.mode_combo)
        load_layout.addRow("", self.create_table_chk)

        # on ajoute les deux, on gère la visibilité
        self.config_area.addWidget(self.extract_widget)
        self.config_area.addWidget(self.load_widget)

        # Boutons bas
        btn_layout = QHBoxLayout()
        self.btn_ok = QPushButton("OK")
        self.btn_cancel = QPushButton("Annuler")

        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_ok)
        btn_layout.addWidget(self.btn_cancel)

        layout.addLayout(btn_layout)

        # Signaux
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

        self.type_combo.currentIndexChanged.connect(self._on_type_changed)
        self.btn_load_tables.clicked.connect(self.on_load_tables_clicked)
        self.tables_list.itemDoubleClicked.connect(self.on_table_double_clicked)

        # État initial
        self._on_type_changed()

    # ------------------------------------------------------------------
    def _on_type_changed(self):
        step_type: StepType = self.type_combo.currentData()
        if step_type == StepType.EXTRACT:
            self.extract_widget.show()
            self.load_widget.hide()
        elif step_type == StepType.LOAD:
            self.extract_widget.hide()
            self.load_widget.show()
        else:
            self.extract_widget.hide()
            self.load_widget.hide()

    def _load_existing(self, step: Step):
        self.name_edit.setText(step.name)

        # Type
        for i in range(self.type_combo.count()):
            if self.type_combo.itemData(i) == step.step_type:
                self.type_combo.setCurrentIndex(i)
                break

        # Connexion
        if step.connection_id is not None:
            for i in range(self.conn_combo.count()):
                if self.conn_combo.itemData(i) == step.connection_id:
                    self.conn_combo.setCurrentIndex(i)
                    break

        # Config spécifique
        if step.step_type == StepType.EXTRACT:
            query = step.config.get("query", "")
            self.query_edit.setPlainText(query)

        elif step.step_type == StepType.LOAD:
            self.table_edit.setText(step.config.get("table", ""))
            mode = step.config.get("mode", "append")
            for i in range(self.mode_combo.count()):
                if self.mode_combo.itemData(i) == mode:
                    self.mode_combo.setCurrentIndex(i)
                    break
            self.create_table_chk.setChecked(step.config.get("create_table", True))

    # ------------------------------------------------------------------
    # Récupérer le Step construit
    # ------------------------------------------------------------------
    def build_step(self, base_order: int, existing_id: Optional[int] = None, task_id: Optional[int] = None) -> Step:

        name = self.name_edit.text().strip()
        step_type: StepType = self.type_combo.currentData()   # << OBLIGATOIRE

        if not name:
            raise ValueError("Le nom de l'étape est obligatoire.")

        conn_id = self.conn_combo.currentData()
        config = {}

        if step_type == StepType.EXTRACT:
            query = self.query_edit.toPlainText().strip()
            if not query:
                raise ValueError("Requête SQL obligatoire pour un EXTRACT.")
            config = {"query": query}

        elif step_type == StepType.LOAD:
            table = self.table_edit.text().strip()
            if not table:
                raise ValueError("Table obligatoire pour un LOAD.")
            mode = self.mode_combo.currentData()
            create_table = self.create_table_chk.isChecked()
            config = {
                "table": table,
                "mode": mode,
                "create_table": create_table,
            }

        return Step(
            id=existing_id,
            task_id=task_id,
            name=name,
            step_type=step_type,   # << doit être un ENUM ici
            order=base_order,
            connection_id=conn_id,
            config=config,
        )

    # ------------------------------------------------------------------
    # Bouton : charger les tables SQL Server
    # ------------------------------------------------------------------
    @Slot()
    def on_load_tables_clicked(self):
        if not self.connections:
            QMessageBox.warning(self, "Connexions", "Aucune connexion définie.")
            return

        conn_id = self.conn_combo.currentData()
        conn_cfg = next((c for c in self.connections if c.id == conn_id), None)

        if not conn_cfg:
            QMessageBox.warning(self, "Connexion", "Connexion non trouvée.")
            return

        if conn_cfg.type != "sqlserver":
            QMessageBox.warning(self, "Connexion", "Liste des tables uniquement pour SQL Server pour l'instant.")
            return

        try:
            extractor = SqlServerExtractor(conn_cfg.params, {})
            tables = extractor.list_tables()
            self.tables_list.clear()
            for t in tables:
                self.tables_list.addItem(QListWidgetItem(t))
        except Exception as e:
            QMessageBox.critical(self, "Erreur", f"Impossible de lister les tables :\n{e}")

    @Slot()
    def on_table_double_clicked(self, item: QListWidgetItem):
        table_name = item.text()
        # insérer un SELECT basique dans la zone SQL
        tpl = f"SELECT * FROM {table_name};"
        self.query_edit.setPlainText(tpl)
