from __future__ import annotations

import pandas as pd
import pyodbc

from services.log_service import get_logger


class SqlServerExtractor:
    """
    Extracteur SQL Server utilisant pyodbc.
    Fournit :
        - extract()
        - list_tables()
        - list_columns()
        - get_table_schema()
        - get_fk_relations_all()
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
    # EXTRACTION DE DONNÉES
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
    # SCHEMA D'UNE TABLE
    # ---------------------------------------------------------
    def get_table_schema(self, table_name: str):
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

        self.log.info(f"Schéma récupéré pour '{table_name}': {len(schema)} colonnes")
        return schema

    # ---------------------------------------------------------
    # LISTE DES TABLES
    # ---------------------------------------------------------
    def list_tables(self):
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
            for _, row in df.iterrows()
        ]

        self.log.info(f"{len(tables)} tables détectées dans SQL Server")
        return tables

    # ---------------------------------------------------------
    # LISTE DES COLONNES
    # ---------------------------------------------------------
    def list_columns(self):
        query = """
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        """

        with self._connect() as conn:
            df = pd.read_sql(query, conn)

        result = {}
        for _, row in df.iterrows():
            table = f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
            if table not in result:
                result[table] = []
            result[table].append({
                "name": row.COLUMN_NAME,
                "type": row.DATA_TYPE,
                "nullable": (row.IS_NULLABLE == "YES"),
                "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                "precision": row.NUMERIC_PRECISION,
                "scale": row.NUMERIC_SCALE,
            })

        self.log.info(f"{len(result)} tables analysées (colonnes).")
        return result

    # ---------------------------------------------------------
    # RELATIONS (FK)
    # ---------------------------------------------------------
    def get_fk_relations_all(self):
        """
        Retourne toutes les relations FK :
        [
            {
                "name": ...,
                "table": ...,
                "column": ...,
                "ref_table": ...,
                "ref_column": ...
            }
        ]
        """
        query = """
        SELECT
            fk.name AS fk_name,
            SCHEMA_NAME(tp.schema_id) + '.' + tp.name AS parent_table,
            cp.name AS column_name,
            SCHEMA_NAME(tr.schema_id) + '.' + tr.name AS ref_table,
            cr.name AS ref_column
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.columns cp ON cp.object_id = tp.object_id AND cp.column_id = fkc.parent_column_id
        INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.columns cr ON cr.object_id = tr.object_id AND cr.column_id = fkc.referenced_column_id
        ORDER BY parent_table, fk.name;
        """

        with self._connect() as conn:
            rows = conn.cursor().execute(query).fetchall()

        rels = []
        for r in rows:
            rels.append({
                "name": r.fk_name,
                "table": r.parent_table,
                "column": r.column_name,
                "ref_table": r.ref_table,
                "ref_column": r.ref_column,
            })

        self.log.info(f"{len(rels)} relations FK détectées.")
        return rels
