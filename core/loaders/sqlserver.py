# core/loaders/sqlserver.py
from __future__ import annotations
import pyodbc
import pandas as pd

from core.utils import sanitize_df_for_sql
from services.log_service import get_logger


class SqlServerLoader:
    """
    Loader SQL Server utilisant pyodbc.

    step.config attendu :
    {
        "table": "dbo.Clients",
        "mode": "append",    # "append" ou "replace"
        "create_table": true # si true → crée la table si manquante
    }

    Connexion :
    {
        "host": "...",
        "port": 1433,
        "user": "...",
        "password": "...",
        "database": "...",
        "driver": "ODBC Driver 17 for SQL Server"
    }
    """

    def __init__(self, conn_params: dict, step_config: dict):
        self.conn_params = conn_params
        self.config = step_config
        self.log = get_logger("SqlServerLoader")

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
        return pyodbc.connect(self._build_connection_string())

    # ---------------------------------------------------------
    # Vérifier si la table existe
    # ---------------------------------------------------------
    def table_exists(self, table_name: str) -> bool:
        schema, table = self._split_table_name(table_name)

        query = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (schema, table))
            exists = cur.fetchone() is not None

        return exists

    def _split_table_name(self, table: str):
        """
        "dbo.Clients" → ("dbo", "Clients")
        """
        if "." in table:
            schema, table_name = table.split(".", 1)
            return schema, table_name
        return "dbo", table

    # ---------------------------------------------------------
    # Transformer DataFrame → SQL types
    # ---------------------------------------------------------
    def _map_dtype(self, dtype) -> str:
        """
        Mapping Pandas → SQL Server types
        """
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        if pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        if pd.api.types.is_bool_dtype(dtype):
            return "BIT"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        return "NVARCHAR(MAX)"  # fallback

    # ---------------------------------------------------------
    # Créer table automatiquement depuis DataFrame
    # ---------------------------------------------------------
    def create_table(self, table_name: str, df: pd.DataFrame):
        schema, table = self._split_table_name(table_name)

        cols_sql = []
        for col in df.columns:
            sql_type = self._map_dtype(df[col].dtype)
            cols_sql.append(f"[{col}] {sql_type}")

        create_sql = f"""
        CREATE TABLE [{schema}].[{table}] (
            {", ".join(cols_sql)}
        );
        """

        self.log.info(f"Création table SQL Server : {table_name}")

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(create_sql)
            conn.commit()

    # ---------------------------------------------------------
    # Charger données dans SQL Server
    # ---------------------------------------------------------
    def load(self, df: pd.DataFrame):
        df = sanitize_df_for_sql(df)
        table_name = self.config.get("table")
        mode = self.config.get("mode", "append").lower()
        auto_create = self.config.get("create_table", True)

        if table_name is None:
            raise ValueError("Step LOAD SQL Server : 'table' manquante")

        schema, table = self._split_table_name(table_name)

        # ---------------------------------------------------------
        # 1) Créer la table si elle n'existe pas
        # ---------------------------------------------------------
        if not self.table_exists(table_name):
            if auto_create:
                self.create_table(table_name, df)
            else:
                raise RuntimeError(f"La table {table_name} n'existe pas.")

        # ---------------------------------------------------------
        # 2) Mode REPLACE : supprimer les données
        # ---------------------------------------------------------
        if mode == "replace":
            self.log.info(f"Mode REPLACE : nettoyage table {table_name}")
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(f"DELETE FROM [{schema}].[{table}]")
                conn.commit()

        # ---------------------------------------------------------
        # 3) Insertion ligne par ligne
        #    (option : on pourra faire BULK plus tard)
        # ---------------------------------------------------------
        placeholders = ", ".join(["?"] * len(df.columns))
        columns = ", ".join([f"[{c}]" for c in df.columns])

        insert_sql = f"""
        INSERT INTO [{schema}].[{table}] ({columns})
        VALUES ({placeholders})
        """

        self.log.info(f"Insertion dans {table_name}, {len(df)} lignes")

        with self._connect() as conn:
            cur = conn.cursor()
            for row in df.itertuples(index=False, name=None):
                cur.execute(insert_sql, row)
            conn.commit()

        self.log.info(f"LOAD SQL Server terminé ({len(df)} lignes).")
