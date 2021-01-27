# flake8: noqa
from typing import Any

import fugue_notebook as fn
from fuggle_version import __version__
from fugue_sql import FugueSQLWorkflow as Dag

from fuggle.execution_engine import (
    KaggleDaskExecutionEngine,
    KaggleNativeExecutionEngine,
    KaggleNotebookSetup,
    KaggleSparkExecutionEngine,
)
from fuggle.outputters import Plot, PlotBar, PlotBarH, PlotLine


def setup(default_engine: str = "") -> Any:
    return fn.setup(KaggleNotebookSetup(default_engine))
