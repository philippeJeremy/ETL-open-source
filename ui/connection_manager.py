# ui/connection_manager.py
from __future__ import annotations

from typing import Optional, List

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QDialog,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QLabel,
    QLineEdit,
    QComboBox,
    QFormLayout,
    QMessageBox,
    QAbstractItemView
)

from services.log_service import get_logger
from storage.repository import Repository
from core.models import ConnectionConfig
from services.connection_service import test_connection


class ConnectionManagerDialog(QDialog):
    """
    Boîte de dialogue pour gérer les connexions BDD.
    """

    def __init__(self, repository: Repository, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.repository = repository
        self.log = get_logger("ConnectionManagerDialog")

        self.setWindowTitle("Gestion des connexions")
        self.resize(700, 400)

        self.connections: List[ConnectionConfig] = []

        self._init_ui()
        self._load_connections()

    # --------------------------------------------------
    # UI
    # --------------------------------------------------
    def _init_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("Connexions aux bases de données")
        title.setStyleSheet("font-size: 18px; font-weight: bold;")
        layout.addWidget(title)

        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Nom", "Type", "Hôte", "Base"])
        self.table.setSelectionBehavior(self.table.SelectRows if hasattr(self.table, "SelectRows") else self.table.selectionBehavior())
        self.table.setSelectionMode(self.table.SingleSelection if hasattr(self.table, "SingleSelection") else self.table.selectionMode())
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)

        layout.addWidget(self.table)

        btn_layout = QHBoxLayout()
        self.btn_add = QPushButton("Ajouter")
        self.btn_edit = QPushButton("Modifier")
        self.btn_delete = QPushButton("Supprimer")
        self.btn_test = QPushButton("Tester la connexion")
        self.btn_close = QPushButton("Fermer")

        btn_layout.addWidget(self.btn_add)
        btn_layout.addWidget(self.btn_edit)
        btn_layout.addWidget(self.btn_delete)
        btn_layout.addWidget(self.btn_test)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_close)

        layout.addLayout(btn_layout)

        # Signaux
        self.btn_add.clicked.connect(self.on_add_clicked)
        self.btn_edit.clicked.connect(self.on_edit_clicked)
        self.btn_delete.clicked.connect(self.on_delete_clicked)
        self.btn_test.clicked.connect(self.on_test_clicked)
        self.btn_close.clicked.connect(self.reject)

    def _load_connections(self):
        self.connections = self.repository.list_connections()
        self._refresh_table()

    def _refresh_table(self):
        self.table.setRowCount(len(self.connections))

        for row, conn in enumerate(self.connections):
            host = conn.params.get("host", "")
            dbname = conn.params.get("database", conn.params.get("dbname", ""))

            self.table.setItem(row, 0, QTableWidgetItem(conn.name))
            self.table.setItem(row, 1, QTableWidgetItem(conn.type))
            self.table.setItem(row, 2, QTableWidgetItem(str(host)))
            self.table.setItem(row, 3, QTableWidgetItem(str(dbname)))

        self.table.resizeColumnsToContents()

    def _get_selected_index(self) -> Optional[int]:
        sel = self.table.selectionModel().selectedRows()
        if not sel:
            return None
        return sel[0].row()

    def _get_selected_connection(self) -> Optional[ConnectionConfig]:
        idx = self._get_selected_index()
        if idx is None or idx < 0 or idx >= len(self.connections):
            return None
        return self.connections[idx]

    # --------------------------------------------------
    # Slots
    # --------------------------------------------------
    @Slot()
    def on_add_clicked(self):
        dialog = ConnectionEditDialog(parent=self)
        if dialog.exec() == QDialog.Accepted:
            cfg = dialog.get_connection_config()
            try:
                self.repository.save_connection(cfg)
                self._load_connections()
            except Exception as e:
                QMessageBox.critical(self, "Erreur", f"Impossible d'enregistrer la connexion :\n{e}")

    @Slot()
    def on_edit_clicked(self):
        conn = self._get_selected_connection()
        if not conn:
            QMessageBox.warning(self, "Aucune sélection", "Sélectionne une connexion.")
            return

        dialog = ConnectionEditDialog(existing=conn, parent=self)
        if dialog.exec() == QDialog.Accepted:
            new_cfg = dialog.get_connection_config()
            new_cfg.id = conn.id  # garder l'ID existant
            try:
                self.repository.save_connection(new_cfg)
                self._load_connections()
            except Exception as e:
                QMessageBox.critical(self, "Erreur", f"Impossible de mettre à jour la connexion :\n{e}")

    @Slot()
    def on_delete_clicked(self):
        conn = self._get_selected_connection()
        if not conn:
            QMessageBox.warning(self, "Aucune sélection", "Sélectionne une connexion.")
            return

        reply = QMessageBox.question(
            self,
            "Confirmer la suppression",
            f"Supprimer la connexion '{conn.name}' ?",
        )
        if reply != QMessageBox.Yes:
            return

        try:
            self.repository.delete_connection(conn.id)
            self._load_connections()
        except Exception as e:
            QMessageBox.critical(self, "Erreur", f"Impossible de supprimer :\n{e}")

    @Slot()
    def on_test_clicked(self):
        conn = self._get_selected_connection()
        if not conn:
            QMessageBox.warning(self, "Aucune sélection", "Sélectionne une connexion.")
            return

        ok, message = test_connection(conn)
        if ok:
            QMessageBox.information(self, "Test de connexion", message)
        else:
            QMessageBox.critical(self, "Test de connexion", message)


# --------------------------------------------------
# Éditeur de connexion individuelle
# --------------------------------------------------
class ConnectionEditDialog(QDialog):
    """
    Boîte de dialogue pour créer / modifier une connexion.
    Pour l'instant, on gère surtout sqlserver.
    """

    def __init__(
        self,
        existing: Optional[ConnectionConfig] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Connexion SQL Server")
        self.resize(400, 300)

        self.existing = existing

        self._init_ui()

        if existing:
            self._load_existing(existing)

    def _init_ui(self):
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.type_combo = QComboBox()
        self.type_combo.addItem("sqlserver")
        # plus tard : postgres, mysql, etc.

        self.host_edit = QLineEdit()
        self.port_edit = QLineEdit()
        self.db_edit = QLineEdit()
        self.user_edit = QLineEdit()
        self.pwd_edit = QLineEdit()
        self.driver_edit = QLineEdit()

        self.port_edit.setText("1433")
        self.driver_edit.setText("ODBC Driver 17 for SQL Server")

        self.pwd_edit.setEchoMode(QLineEdit.Password)

        form.addRow("Nom :", self.name_edit)
        form.addRow("Type :", self.type_combo)
        form.addRow("Hôte :", self.host_edit)
        form.addRow("Port :", self.port_edit)
        form.addRow("Base :", self.db_edit)
        form.addRow("Utilisateur :", self.user_edit)
        form.addRow("Mot de passe :", self.pwd_edit)
        form.addRow("Driver ODBC :", self.driver_edit)

        layout.addLayout(form)

        btn_layout = QHBoxLayout()
        self.btn_ok = QPushButton("Enregistrer")
        self.btn_cancel = QPushButton("Annuler")

        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_ok)
        btn_layout.addWidget(self.btn_cancel)

        layout.addLayout(btn_layout)

        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

    def _load_existing(self, cfg: ConnectionConfig):
        self.name_edit.setText(cfg.name)
        index = self.type_combo.findText(cfg.type)
        if index >= 0:
            self.type_combo.setCurrentIndex(index)

        params = cfg.params
        self.host_edit.setText(str(params.get("host", "")))
        self.port_edit.setText(str(params.get("port", 1433)))
        self.db_edit.setText(str(params.get("database", params.get("dbname", ""))))
        self.user_edit.setText(str(params.get("user", "")))
        self.pwd_edit.setText(str(params.get("password", "")))
        self.driver_edit.setText(str(params.get("driver", "ODBC Driver 17 for SQL Server")))

    def get_connection_config(self) -> ConnectionConfig:
        name = self.name_edit.text().strip()
        ctype = self.type_combo.currentText()

        params = {
            "host": self.host_edit.text().strip(),
            "port": int(self.port_edit.text().strip() or 1433),
            "database": self.db_edit.text().strip(),
            "user": self.user_edit.text().strip(),
            "password": self.pwd_edit.text().strip(),
            "driver": self.driver_edit.text().strip(),
        }

        return ConnectionConfig(
            id=None,
            name=name,
            type=ctype,
            params=params,
        )
