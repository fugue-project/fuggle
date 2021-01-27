import os

import pytest
from fuggle import (
    KaggleNativeExecutionEngine,
    KaggleSparkExecutionEngine,
    KaggleDaskExecutionEngine,
)
from fugue_sql import FugueSQLWorkflow
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pyspark.sql import SparkSession


class KaggleNativeExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = KaggleNativeExecutionEngine(conf={"test": True})
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class KaggleNativeExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = KaggleNativeExecutionEngine(
            conf={
                "test": True,
                "fuggle.sqlite.path": os.path.join(os.getcwd(), "tests/data"),
            }
        )
        return e

    def dag(self) -> FugueSQLWorkflow:
        return FugueSQLWorkflow(self.engine)

    def test_sqlite(self):
        with self.dag() as dag:
            dag(
                """
            SELECT COUNT(*) AS ct FROM customer.sqlite.customer
            PRINT
            """
            )


class KaggleSparkExecutionEngineTests(ExecutionEngineTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        session = SparkSession.builder.getOrCreate()
        e = KaggleSparkExecutionEngine(spark_session=session, conf={"test": True})
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return

    def test__join_outer_pandas_incompatible(self):
        return

    def test_sample_n(self):
        pass


class KaggleSparkExecutionEngineBuiltInTests(BuiltInTests.Tests):
    @pytest.fixture(autouse=True)
    def init_session(self, spark_session):
        self.spark_session = spark_session

    def make_engine(self):
        e = KaggleSparkExecutionEngine(
            conf={
                "test": True,
                "fuggle.sqlite.path": os.path.join(os.getcwd(), "tests/data"),
            }
        )
        return e

    def dag(self) -> FugueSQLWorkflow:
        return FugueSQLWorkflow(self.engine)

    def test_callback(self):
        pass

    def test_sqlite(self):
        with self.dag() as dag:
            dag(
                """
            SELECT COUNT(*) AS ct FROM customer.sqlite.customer
            PRINT
            """
            )

    def test_repartition(self):
        pass


class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = KaggleDaskExecutionEngine(
            conf={
                "test": True,
                "fuggle.sqlite.path": os.path.join(os.getcwd(), "tests/data"),
            }
        )
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_map_with_dict_col(self):
        # TODO: add back
        return

    def test_sample_n(self):
        pass


class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = KaggleDaskExecutionEngine(
            conf={
                "test": True,
                "fuggle.sqlite.path": os.path.join(os.getcwd(), "tests/data"),
            }
        )
        return e

    def dag(self) -> FugueSQLWorkflow:
        return FugueSQLWorkflow(self.engine)

    def test_sqlite(self):
        with self.dag() as dag:
            dag(
                """
            SELECT COUNT(*) AS ct FROM customer.sqlite.customer
            PRINT
            """
            )

    def test_repartition(self):
        pass
