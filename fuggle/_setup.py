# flake8: noqa
import html
from datetime import datetime
from typing import Any, List, Optional, Dict, Tuple

import pandas as pd
from fugue import ExecutionEngine, Schema
from fugue.extensions._builtins.outputters import Show
from fugue_spark import SparkExecutionEngine
from fugue_sql import FugueSQLWorkflow
from IPython.core.magic import register_cell_magic
from IPython.display import HTML, Javascript, display
from pyspark.sql import SparkSession
from triad.utils.convert import get_caller_global_local_vars, to_instance

from fuggle.execution_engine import KaggleNativeExecutionEngine
import inspect


class EngineFactory(object):
    def __init__(self, default_engine: Any = None):
        self._default_engine = self.make_engine(default_engine)

    def make_engine(self, engine: Any) -> ExecutionEngine:
        if engine is None or (isinstance(engine, str) and engine in ["native", ""]):
            return KaggleNativeExecutionEngine(use_sqlite=False)
        if isinstance(engine, str) and engine == "spark":
            spark_session = (
                SparkSession.builder.master("local[*]")
                .config("spark.driver.memory", "12g")
                .config("fugue.spark.use_pandas_udf", True)
                .getOrCreate()
            )
            return SparkExecutionEngine(spark_session)
        return to_instance(engine, ExecutionEngine)

    @property
    def default_engine(self) -> ExecutionEngine:
        return self._default_engine


ENGINE_FACTORY = EngineFactory("native")


HIGHLIGHT_JS = """
require(["codemirror/lib/codemirror"]);

function set(str) {
    var obj = {}, words = str.split(" ");
    for (var i = 0; i < words.length; ++i) obj[words[i]] = true;
    return obj;
  }

var fugue_keywords = "fill hash rand even presort persist broadcast params process output outtransform rowcount concurrency prepartition zip print title save append parquet csv json single checkpoint weak strong deterministic yield";

CodeMirror.defineMIME("text/x-mssql", {
    name: "sql",
    keywords: set(fugue_keywords + " add after all alter analyze and anti archive array as asc at between bucket buckets by cache cascade case cast change clear cluster clustered codegen collection column columns comment commit compact compactions compute concatenate cost create cross cube current current_date current_timestamp database databases datata dbproperties defined delete delimited deny desc describe dfs directories distinct distribute drop else end escaped except exchange exists explain export extended external false fields fileformat first following for format formatted from full function functions global grant group grouping having if ignore import in index indexes inner inpath inputformat insert intersect interval into is items join keys last lateral lazy left like limit lines list load local location lock locks logical macro map minus msck natural no not null nulls of on optimize option options or order out outer outputformat over overwrite partition partitioned partitions percent preceding principals purge range recordreader recordwriter recover reduce refresh regexp rename repair replace reset restrict revoke right rlike role roles rollback rollup row rows schema schemas select semi separated serde serdeproperties set sets show skewed sort sorted start statistics stored stratify struct table tables tablesample tblproperties temp temporary terminated then to touch transaction transactions transform true truncate unarchive unbounded uncache union unlock unset use using values view when where window with"),
    builtin: set("tinyint smallint int bigint boolean float double string binary timestamp decimal array map struct uniontype delimited serde sequencefile textfile rcfile inputformat outputformat"),
    atoms: set("false true null unknown"),
    operatorChars: /^[*\/+\-%<>!=&|^\/#@?~]/,
    dateSQL: set("datetime date time timestamp"),
    support: set("ODBCdotTable doubleQuote binaryNumber hexNumber commentSlashSlash commentHash")
  });


require(['notebook/js/codecell'], function(codecell) {
    codecell.CodeCell.options_default.highlight_modes['magic_text/x-mssql'] = {'reg':[/%%fsql/]} ;
    Jupyter.notebook.events.one('kernel_ready.Kernel', function(){
    Jupyter.notebook.get_cells().map(function(cell){
        if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
  });

"""


def register_magic(default_engine: Any) -> None:
    engine = ENGINE_FACTORY.make_engine(default_engine)
    display(HTML(f"<strong>{engine} is set as backend<strong>"))

    @register_cell_magic
    def fsql(line: Any, cell: Any) -> None:  # type: ignore
        start = datetime.now()
        try:
            global_vars = _get_caller_global_vars()
            dag = FugueSQLWorkflow()
            dag(cell, global_vars)
            dag.run(engine if line == "" else ENGINE_FACTORY.make_engine(line))
        finally:
            sec = (datetime.now() - start).total_seconds()
            display(HTML(f"<small><u>{sec} seconds</u></small>"))


def _get_caller_global_vars(
    global_vars: Optional[Dict[str, Any]] = None,
    max_depth: int = 10,
) -> Dict[str, Any]:
    cf = inspect.currentframe()
    stack: Any = cf.f_back.f_back  # type: ignore
    while stack is not None and max_depth > 0:
        if global_vars is None:
            global_vars = stack.f_globals  # type: ignore
        else:
            global_vars.update(stack.f_globals)
        stack = stack.f_back  # type: ignore
        max_depth -= 1
    return global_vars  # type: ignore


def set_print_hook() -> None:
    def pprint(
        schema: Schema, head_rows: List[List[Any]], title: Any, rows: int, count: int
    ):
        if title is not None:
            display(HTML(f"<h3>{html.escape(title)}</h3>"))
        pdf = pd.DataFrame(head_rows, columns=list(schema.names))
        display(pdf)
        if count >= 0:
            display(HTML(f"<strong>total count: {count}</strong>"))
        display(HTML(f"<small>schema: {schema}</small>"))

    Show.set_hook(pprint)


def setup(default_engine: Any = None) -> Any:
    register_magic(default_engine)
    set_print_hook()
    return Javascript(HIGHLIGHT_JS)
