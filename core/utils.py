import datetime
import pandas as pd
import numpy as np
from pandas._libs.tslibs.nattype import NaTType

def sanitize_df_for_sql(df: pd.DataFrame):
        def fix_value(x):
            if isinstance(x, NaTType):
                return datetime.datetime(1900, 1, 1)
            elif isinstance(x, pd.Timestamp):
                return x.to_pydatetime().replace(microsecond=0)
            elif pd.isna(x):
                return ""
            return x
        return df.map(fix_value)
