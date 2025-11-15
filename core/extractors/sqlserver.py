# core/extractors/sqlserver.py
from __future__ import annotations

import pandas as pd
import pyodbc

from services.log_service import get_logger


class SqlServerExtractor:
    """
    Extracteur SQL Server utilisant pyodbc.

    step.config :
        - query : "SELECT ... "
        - table : "dbo.Clients"   (pour introspection du schéma)
    """

    def __init__(self, conn_params: dict, step_config: dict):
        self.conn_params = conn_params
        self.step_config = step_config
        self.log = get_logger("SqlServerExtractor")

    # ---------------------------------------------------------
    # Connexion SQL Server
    # ---------------------------------------------------------
    def _build_connection_string(self) -> str:
        driver = self.conn_params.get("driver", "ODBC Driver 17 for SQL Server")

        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.conn_params['host']},{self.conn_params.get('port', 1433)};"
            f"DATABASE={self.conn_params['database']};"
            f"UID={self.conn_params['user']};"
            f"PWD={self.conn_params['password']}"
        )

    def _connect(self):
        conn_str = self._build_connection_string()
        return pyodbc.connect(conn_str)

    # ---------------------------------------------------------
    # 1) EXTRACTION DE DONNÉES
    # ---------------------------------------------------------
    def extract(self):
        query = self.step_config.get("query")

        if not query:
            raise ValueError("Step EXTRACT (SQL Server) : 'query' manquante")

        with self._connect() as conn:
            df = pd.read_sql(query, conn)
            self.log.info(f"{len(df)} lignes extraites depuis SQL Server")
            return df

    # ---------------------------------------------------------
    # 2) EXTRACTION DU SCHÉMA D'UNE TABLE
    # ---------------------------------------------------------
    def get_table_schema(self, table_name: str):
        """
        Retourne le schéma d'une table sous forme de liste de dictionnaires.

        Exemple :
        [
            {"name": "id", "type": "int", "nullable": False, "max_length": None},
            {"name": "firstname", "type": "varchar", "nullable": True, "max_length": 50},
            ...
        ]
        """
        query = """
        SELECT 
            c.name AS column_name,
            t.Name AS type_name,
            c.max_length,
            c.is_nullable
        FROM sys.columns c
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN sys.objects o ON c.object_id = o.object_id
        WHERE o.name = ?;
        """

        table_only = table_name.split(".")[-1]

        with self._connect() as conn:
            cursor = conn.cursor()
            rows = cursor.execute(query, table_only).fetchall()

        schema = [
            {
                "name": row.column_name,
                "type": row.type_name,
                "nullable": bool(row.is_nullable),
                "max_length": row.max_length,
            }
            for row in rows
        ]

        self.log.info(f"Schéma récupéré pour la table '{table_name}': {len(schema)} colonnes")
        return schema

    # ---------------------------------------------------------
    # 3) LISTE DES TABLES DANS LA BASE
    # ---------------------------------------------------------
    def list_tables(self):
        """
        Retourne la liste des tables disponibles dans la base.
        """
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME;
        """

        with self._connect() as conn:
            df = pd.read_sql(query, conn)

        tables = [
            f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
            for idx, row in df.iterrows()
        ]

        self.log.info(f"{len(tables)} tables détectées dans SQL Server")
        return tables
