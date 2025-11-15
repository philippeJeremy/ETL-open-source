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
        "mode": "append",           # "append" ou "replace"
        "create_table": true,       # si true → crée la table si manquante
        # optionnel : si tu veux copier le schéma depuis une table source
        # "source_table": "dbo.Clients",
        # "source_connection_params": { ... }  # même format que conn_params
    }
    """

    def __init__(self, conn_params: dict, step_config: dict):
        self.conn_params = conn_params      # cible
        self.config = step_config
        self.log = get_logger("SqlServerLoader")

    # ---------------------------------------------------------
    # Connexion SQL Server
    # ---------------------------------------------------------
    def _build_connection_string(self, params: dict) -> str:
        driver = params.get("driver", "ODBC Driver 17 for SQL Server")
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={params['host']},{params.get('port', 1433)};"
            f"DATABASE={params['database']};"
            f"UID={params['user']};"
            f"PWD={params['password']}"
        )

    def _connect_target(self):
        return pyodbc.connect(self._build_connection_string(self.conn_params))

    def _connect_source(self, source_conn_params: dict):
        return pyodbc.connect(self._build_connection_string(source_conn_params))

    # ---------------------------------------------------------
    # Outillage noms schema/table
    # ---------------------------------------------------------
    def _split_table_name(self, table: str):
        """
        "dbo.Clients" -> ("dbo", "Clients")
        """
        if "." in table:
            schema, table_name = table.split(".", 1)
            return schema, table_name
        return "dbo", table

    # ---------------------------------------------------------
    # Vérifier si la table existe
    # ---------------------------------------------------------
    def table_exists(self, target_table: str) -> bool:
        schema, table = self._split_table_name(target_table)

        query = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """

        with self._connect_target() as conn:
            cur = conn.cursor()
            cur.execute(query, (schema, table))
            exists = cur.fetchone() is not None

        return exists

    # ---------------------------------------------------------
    # 1) Vérifier et créer la table si absente
    # ---------------------------------------------------------
    def ensure_table_exists(
        self,
        df: pd.DataFrame,
        target_table: str,
    ):
        """
        Vérifie si la table existe sur la BDD cible.
        Si elle n'existe pas :
            - tente de générer le schéma depuis une table source (facultatif)
            - sinon, fallback : mapping pandas -> SQL Server
        """
        schema, table = self._split_table_name(target_table)

        if self.table_exists(target_table):
            self.log.info(
                f"Table {target_table} déjà existante — aucune création nécessaire")
            return

        self.log.info(
            f"Table {target_table} absente — création en cours...")

        # Optionnel : récupérer info depuis une table source
        source_conn_params = self.config.get("source_connection_params")
        source_table = self.config.get("source_table", target_table)

        create_sql = self._generate_create_table_sql(
            df=df,
            schema=schema,
            table=table,
            source_conn_params=source_conn_params,
            source_table=source_table,
        )

        self.log.info(f"CREATE TABLE généré :\n{create_sql}")

        with self._connect_target() as conn:
            cur = conn.cursor()
            cur.execute(create_sql)
            conn.commit()

        self.log.info(f"Table {target_table} créée avec succès")

    # ---------------------------------------------------------
    # 2) Génération du CREATE TABLE
    # ---------------------------------------------------------
    def _generate_create_table_sql(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        source_conn_params: dict | None = None,
        source_table: str | None = None,
    ) -> str:
        """
        Génère le CREATE TABLE basé sur les métadonnées d'une table source
        (si source_conn_params est fourni), sinon fallback sur les types pandas.
        """

        # ----------------------------
        # 1) Try : schéma depuis table source (INFORMATION_SCHEMA)
        # ----------------------------
        if source_conn_params and source_table:
            try:
                self.log.info(
                    f"Récupération du schéma source pour {source_table}...")

                if "." in source_table:
                    src_schema, src_table = source_table.split(".", 1)
                else:
                    src_schema, src_table = "dbo", source_table

                query = f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """

                with self._connect_source(source_conn_params) as src_conn:
                    schema_df = pd.read_sql(
                        query, src_conn, params=[src_schema, src_table])

                if not schema_df.empty:
                    # garder uniquement les colonnes présentes dans le DataFrame
                    selected_cols = list(df.columns)
                    schema_df = schema_df[schema_df["COLUMN_NAME"].isin(
                        selected_cols)]

                    if schema_df.empty:
                        self.log.warning(
                            f"Aucune colonne correspondante trouvée dans le schéma source pour {src_schema}.{src_table}. Fallback générique.")
                        raise ValueError(
                            "Aucune correspondance de colonnes dans le schéma source.")

                    column_defs = []

                    for _, row in schema_df.iterrows():
                        col = row["COLUMN_NAME"]
                        data_type = row["DATA_TYPE"]
                        nullable = "NULL" if row["IS_NULLABLE"] == "YES" else "NOT NULL"

                        # Longueur / précision / échelle
                        if data_type in ("varchar", "nvarchar", "char", "nchar"):
                            length = row["CHARACTER_MAXIMUM_LENGTH"]
                            if pd.isna(length):
                                pass
                            elif int(length) == -1:
                                data_type = f"{data_type}(MAX)"
                            else:
                                data_type = f"{data_type}({int(length)})"
                        elif data_type in ("decimal", "numeric"):
                            prec = int(row["NUMERIC_PRECISION"] or 18)
                            scale = int(row["NUMERIC_SCALE"] or 0)
                            data_type = f"{data_type}({prec},{scale})"

                        column_defs.append(f"[{col}] {data_type} {nullable}")

                    columns_str = ",\n    ".join(column_defs)
                    return f"CREATE TABLE [{schema}].[{table}] (\n    {columns_str}\n)"

                else:
                    self.log.warning(
                        f"Aucun schéma trouvé dans INFORMATION_SCHEMA pour {src_schema}.{src_table}, fallback générique...")

            except Exception as e:
                self.log.warning(
                    f"Erreur récupération schéma source ({e}) — fallback générique...")

        # ----------------------------
        # 2) Fallback : mapping pandas -> SQL Server
        # ----------------------------
        self.log.info(
            "Utilisation du fallback pandas -> SQL Server pour générer le CREATE TABLE.")

        type_mapping = {
            "int64": "INT",
            "Int64": "INT",
            "float64": "FLOAT",
            "bool": "BIT",
            "datetime64[ns]": "SMALLDATETIME",
            "object": "NVARCHAR(MAX)",
        }

        columns_sql = []
        for col, dtype in df.dtypes.items():
            dt_str = str(dtype)
            sql_type = type_mapping.get(dt_str, "NVARCHAR(MAX)")
            columns_sql.append(f"[{col}] {sql_type}")

        columns_str = ",\n    ".join(columns_sql)
        return f"CREATE TABLE [{schema}].[{table}] (\n    {columns_str}\n)"

    # ---------------------------------------------------------
    # 3) LOAD principal
    # ---------------------------------------------------------
    def load(self, df: pd.DataFrame):
        df = sanitize_df_for_sql(df)
        table_name = self.config.get("table")
        mode = self.config.get("mode", "append").lower()
        auto_create = self.config.get("create_table", True)

        if table_name is None:
            raise ValueError("Step LOAD SQL Server : 'table' manquante")

        # 1) Création si besoin
        if auto_create:
            self.ensure_table_exists(df, table_name)
        else:
            if not self.table_exists(table_name):
                raise RuntimeError(
                    f"La table {table_name} n'existe pas et create_table=False")

        schema, table = self._split_table_name(table_name)

        # 2) Mode replace : purge
        if mode == "replace":
            self.log.info(f"Mode REPLACE : nettoyage table {table_name}")
            with self._connect_target() as conn:
                cur = conn.cursor()
                cur.execute(f"DELETE FROM [{schema}].[{table}]")
                conn.commit()

        # 3) Insert des lignes
        placeholders = ", ".join(["?"] * len(df.columns))
        columns = ", ".join([f"[{c}]" for c in df.columns])

        insert_sql = f"""
        INSERT INTO [{schema}].[{table}] ({columns})
        VALUES ({placeholders})
        """

        self.log.info(f"Insertion dans {table_name}, {len(df)} lignes")

        with self._connect_target() as conn:
            cur = conn.cursor()
            for row in df.itertuples(index=False, name=None):
                cur.execute(insert_sql, row)
            conn.commit()

        self.log.info(f"LOAD SQL Server terminé ({len(df)} lignes).")
