from typing import Any

from fugue import (
    DataFrame,
    DataFrames,
    ExecutionEngine,
    NativeExecutionEngine,
    PandasDataFrame,
    SQLEngine,
)
from qpd_pandas import run_sql_on_pandas


class QPDPandasEngine(SQLEngine):
    """QPD execution implementation.
    :param execution_engine: the execution engine this sql engine will run on
    """

    def __init__(self, execution_engine: ExecutionEngine):
        super().__init__(execution_engine)

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        pd_dfs = {k: self.execution_engine.to_df(v).as_pandas() for k, v in dfs.items()}
        df = run_sql_on_pandas(statement, pd_dfs)
        return PandasDataFrame(df)


class KaggleNativeExecutionEngine(NativeExecutionEngine):
    def __init__(self, conf: Any = None, use_sqlite: bool = False):
        super().__init__(conf)
        if not use_sqlite:
            self._default_sql_engine = QPDPandasEngine(self)
