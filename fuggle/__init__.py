# flake8: noqa
from fuggle_version import __version__

from fuggle._setup import setup
from fuggle.execution_engine import (
    KaggleDaskExecutionEngine,
    KaggleNativeExecutionEngine,
    KaggleSparkExecutionEngine,
)
from fuggle.outputters import Plot, PlotBar, PlotBarH, PlotLine
from fuggle.workflow import Dag
