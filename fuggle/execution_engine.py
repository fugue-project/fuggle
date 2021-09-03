import os
import sqlite3
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd
import psutil
from fugue import (
    DataFrame,
    DataFrames,
    ExecutionEngine,
    NativeExecutionEngine,
    PandasDataFrame,
    SQLEngine,
    SqliteEngine,
    register_default_execution_engine,
    register_execution_engine,
)
from fugue.constants import FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH
from fugue_dask._constants import FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS
from fugue_dask.execution_engine import DaskExecutionEngine, QPDDaskEngine
from fugue_notebook import NotebookSetup
from fugue_spark.execution_engine import SparkExecutionEngine, SparkSQLEngine
from pyspark.sql import SparkSession
from qpd_pandas import run_sql_on_pandas
from triad import ParamDict
from triad.utils.assertion import assert_or_throw

from fuggle._utils import transform_sqlite_sql

DEFAULT_KAGGLE_SQLITE_PATH = ""
BASE_CONFIG: Dict[str, Any] = {
    FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH: "/tmp",
    "fugue.rpc.server": "fugue.rpc.base.NativeRPCServer",
}


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


class KaggleSQLEngineWrapper(SQLEngine):
    def __init__(self, execution_engine: ExecutionEngine, engine: SQLEngine):
        super().__init__(execution_engine)
        self.engine = engine
        self.database_path = execution_engine.conf.get(
            "fuggle.sqlite.path", "/kaggle/input"
        )

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        sql, sqlite_file = transform_sqlite_sql(statement, self._validate_database)
        if sqlite_file is None:
            return self.engine.select(dfs, statement)
        assert_or_throw(len(dfs) == 0, "sql to query sqlite can't have other tables")
        with sqlite3.connect(
            os.path.join(self.database_path, sqlite_file)
        ) as connection:
            df = pd.read_sql_query(sql, connection)
        return PandasDataFrame(df)

    def _validate_database(self, name: str):
        path = os.path.join(self.database_path, name)
        assert_or_throw(self.execution_engine.fs.exists(path), FileNotFoundError(path))


class KaggleNativeExecutionEngine(NativeExecutionEngine):
    def __init__(self, conf: Any = None, use_sqlite: bool = False):
        configs = _process_confs(ParamDict(conf))
        super().__init__(configs)
        if not use_sqlite:
            self.set_sql_engine(KaggleSQLEngineWrapper(self, QPDPandasEngine(self)))
        else:  # pragma: no cover
            self.set_sql_engine(KaggleSQLEngineWrapper(self, SqliteEngine(self)))


class KaggleSparkExecutionEngine(SparkExecutionEngine):
    def __init__(self, spark_session: Optional[SparkSession] = None, conf: Any = None):
        configs = _process_confs(
            {
                "fugue.spark.use_pandas_udf": True,
                "spark.driver.memory": _get_optimal_mem(),
                "spark.sql.shuffle.partitions": _get_optimal_partition(),
                "spark.sql.execution.arrow.pyspark.fallback.enabled": True,
                "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",  # noqa: E501
                "spark.executor.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",  # noqa: E501
            },
            ParamDict(conf),
        )
        builder = SparkSession.builder.master("local[*]")
        for k, v in configs.items():
            builder = builder.config(k, v)
        spark_session = builder.getOrCreate()
        super().__init__(spark_session=spark_session, conf=configs)
        self.set_sql_engine(KaggleSQLEngineWrapper(self, SparkSQLEngine(self)))


class KaggleDaskExecutionEngine(DaskExecutionEngine):
    def __init__(self, conf: Any = None):
        configs = _process_confs(
            {FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS: _get_optimal_partition()},
            ParamDict(conf),
        )
        super().__init__(conf=configs)
        try:
            from dask_sql.integrations.fugue import DaskSQLEngine

            self.set_sql_engine(KaggleSQLEngineWrapper(self, DaskSQLEngine(self)))
            print("dask-sql is set as the SQL Engine for Dask")
        except ImportError:
            self.set_sql_engine(KaggleSQLEngineWrapper(self, QPDDaskEngine(self)))


class KaggleNotebookSetup(NotebookSetup):
    def __init__(self, default_engine: str = ""):
        self._default_engine = default_engine

    def get_pre_conf(self) -> Dict[str, Any]:
        return BASE_CONFIG

    def register_execution_engines(self):
        super().register_execution_engines()
        register_execution_engine(
            "native",
            lambda conf, **kwargs: KaggleNativeExecutionEngine(conf=conf, **kwargs),
        )
        if self._default_engine in ["native", ""]:
            register_default_execution_engine(
                lambda conf, **kwargs: KaggleNativeExecutionEngine(conf=conf, **kwargs)
            )
        register_execution_engine(
            "dask",
            lambda conf, **kwargs: KaggleDaskExecutionEngine(conf=conf),
        )
        if self._default_engine == "dask":
            register_default_execution_engine(
                lambda conf, **kwargs: KaggleDaskExecutionEngine(conf=conf),
            )
        register_execution_engine(
            "spark",
            lambda conf, **kwargs: KaggleSparkExecutionEngine(conf=conf),
        )
        if self._default_engine == "spark":
            register_default_execution_engine(
                lambda conf, **kwargs: KaggleSparkExecutionEngine(conf=conf),
            )


def _process_confs(*confs: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for conf in confs:
        for k, v in _process_conf(conf):
            result[k] = v
    return result


def _process_conf(conf: Dict[str, Any]) -> Iterable[Tuple[str, Any]]:
    for k, v in conf.items():
        if k == "callback":
            if v:
                yield "fugue.rpc.server", "fugue.rpc.flask.FlaskRPCServer"
                yield "fugue.rpc.flask_server.host", "127.0.0.1"
                yield "fugue.rpc.flask_server.port", "7777"
                yield "fugue.rpc.flask_server.timeout", "5 sec"
            else:
                yield "fugue.rpc.server", "fugue.rpc.base.NativeRPCServer"
        else:
            yield k, v


def _get_optimal_mem(ratio: float = 0.8) -> str:
    mem = psutil.virtual_memory()
    return str(int(mem.total * ratio / 1024 / 1024)) + "m"


def _get_optimal_partition() -> int:
    return psutil.cpu_count() * 4
