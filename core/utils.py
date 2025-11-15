import datetime
import pandas as pd
import numpy as np
from pandas._libs.tslibs.nattype import NaTType

def sanitize_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:

    def sanitize_value(x):
        # Valeurs datetime manquantes
        if isinstance(x, NaTType):
            return datetime.datetime(1900, 1, 1)

        # Timestamp Pandas → datetime Python (sans microsecondes)
        if isinstance(x, pd.Timestamp):
            return x.to_pydatetime().replace(microsecond=0)

        # NaN / None
        if pd.isna(x):
            return None  # SQL Server accepte mieux NULL que ''

        # numpy type → convertir en type Python
        if isinstance(x, np.generic):
            return x.item()

        return x

    return df.applymap(sanitize_value)
