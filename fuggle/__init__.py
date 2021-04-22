# flake8: noqa
from typing import Any, Optional

import fugue_notebook as fn
from fuggle_version import __version__
from fugue_sql import FugueSQLWorkflow as Dag
from tune import TUNE_OBJECT_FACTORY, Monitor, NonIterativeObjectiveRunner
from tune_hyperopt import HyperoptRunner
from tune_notebook.vis import (
    NotebookSimpleHist,
    NotebookSimpleRungs,
    NotebookSimpleTimeSeries,
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

    return fn.setup(KaggleNotebookSetup(default_engine))


def _to_runner(obj: Any) -> Optional[NonIterativeObjectiveRunner]:
    if obj is None:
        return HyperoptRunner(20, 0)
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
    raise NotImplementedError(obj)
