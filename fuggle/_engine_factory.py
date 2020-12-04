from typing import Any

from fugue import ExecutionEngine
from fugue_dask._constants import FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS
from pyspark.sql import SparkSession
from triad import ParamDict
from triad.utils.convert import to_instance

from fuggle.execution_engine import (
    KaggleDaskExecutionEngine,
    KaggleNativeExecutionEngine,
    KaggleSparkExecutionEngine,
)


class EngineFactory(object):
    def __init__(self, default_engine: Any, conf: Any = None):
        self._default_engine = self.make_engine(default_engine, conf)

    def make_engine(self, engine: Any, conf: Any) -> ExecutionEngine:
        if engine is None or (isinstance(engine, str) and engine in ["native", ""]):
            return KaggleNativeExecutionEngine(conf=conf, use_sqlite=False)
        if isinstance(engine, str) and engine == "spark":
            configs = {
                "spark.driver.memory": "14g",
                "spark.sql.shuffle.partitions": "16",
                "fugue.spark.use_pandas_udf": True,
            }
            configs.update(ParamDict(conf))
            builder = SparkSession.builder.master("local[*]")
            for k, v in configs.items():
                builder = builder.config(k, v)
            spark_session = builder.getOrCreate()
            return KaggleSparkExecutionEngine(spark_session)
        if isinstance(engine, str) and engine == "dask":
            configs = {FUGUE_DASK_CONF_DATAFRAME_DEFAULT_PARTITIONS: 16}
            return KaggleDaskExecutionEngine(conf=configs)
        return to_instance(engine, ExecutionEngine)

    @property
    def default_engine(self) -> ExecutionEngine:
        return self._default_engine


ENGINE_FACTORY = EngineFactory("native")
