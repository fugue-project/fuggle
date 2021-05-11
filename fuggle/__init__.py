# flake8: noqa
from typing import Any, Optional

import fugue_notebook as fn
from fuggle_version import __version__
from fugue_sql import FugueSQLWorkflow as Dag
from IPython import get_ipython
from tune import TUNE_OBJECT_FACTORY, Monitor, NonIterativeObjectiveLocalOptimizer
from tune_hyperopt import HyperoptLocalOptimizer
from tune_notebook import (
    NotebookSimpleHist,
    NotebookSimpleRungs,
    NotebookSimpleTimeSeries,
    PrintBest,
)
from fuggle.execution_engine import (
    KaggleDaskExecutionEngine,
    KaggleNativeExecutionEngine,
    KaggleNotebookSetup,
    KaggleSparkExecutionEngine,
)
from fuggle.outputters import Plot, PlotBar, PlotBarH, PlotLine


def setup(default_engine: str = "") -> Any:
    TUNE_OBJECT_FACTORY.set_temp_path("/tmp")
    TUNE_OBJECT_FACTORY.set_noniterative_objective_runner_converter(_to_runner)
    TUNE_OBJECT_FACTORY.set_monitor_converter(_to_monitor)

    # we no longer enable SQL highlighting, kaggle has changed
    ip = get_ipython()
    fn._setup_fugue_notebook(ip, KaggleNotebookSetup(default_engine))


def _to_runner(obj: Any) -> Optional[NonIterativeObjectiveLocalOptimizer]:
    if obj is None:
        return HyperoptLocalOptimizer(20, 0)
    raise NotImplementedError(obj)


def _to_monitor(obj: Any) -> Optional[Monitor]:
    if obj is None:
        return None
    if isinstance(obj, str):
        if obj == "hist":
            return NotebookSimpleHist()
        if obj == "rungs":
            return NotebookSimpleRungs()
        if obj == "ts":
            return NotebookSimpleTimeSeries()
        if obj == "text":
            return PrintBest()
    raise NotImplementedError(obj)
