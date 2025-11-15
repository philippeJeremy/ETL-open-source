# services/connection_service.py
from __future__ import annotations

import pyodbc
from typing import Tuple
from services.log_service import get_logger
from core.models import ConnectionConfig


def _build_sqlserver_conn_str(params: dict) -> str:
    return (
        f"DRIVER={{{params.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"SERVER={params['host']},{params.get('port', 1433)};"
        f"DATABASE={params['database']};"
        f"UID={params['user']};"
        f"PWD={params['password']}"
    )


def test_connection(conn_cfg: ConnectionConfig) -> Tuple[bool, str]:
    """
    Teste une connexion SQL Server.
    Retourne (ok: bool, message: str)
    """

    log = get_logger("ConnectionService")  # <<< MOVE HERE !!!!

    log.info(f"Test de connexion : {conn_cfg.name} (type={conn_cfg.type})")

    if conn_cfg.type == "sqlserver":
        try:
            conn_str = _build_sqlserver_conn_str(conn_cfg.params)
            with pyodbc.connect(conn_str, timeout=5):
                pass
            return True, "Connexion SQL Server OK"
        except Exception as e:
            log.error(f"Erreur de connexion SQL Server : {e}")
            return False, str(e)

    return False, f"Type non supporté : {conn_cfg.type}"
