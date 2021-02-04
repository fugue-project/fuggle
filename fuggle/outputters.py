import math
from typing import Any, List, Tuple, Dict

from matplotlib.gridspec import GridSpec
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from fugue import DataFrames, Outputter, DataFrame
from fugue.collections.partition import parse_presort_exp
import pandas as pd
from triad.utils.assertion import assert_or_throw


class Plot(Outputter):
    @property
    def kind(self) -> str:
        return ""

    def validate_on_compile(self) -> None:
        if self.kind == "":
            self.params.get_or_throw("kind", str)
        else:
            assert_or_throw("kind" not in self.params, f"can't reset kind {self.kind}")
        self.params.get("top_n", 0)
        parse_presort_exp(self.params.get("order_by", "a"))
        self.params.get_or_throw("x", str)
        y = self.params.get_or_none("y", object)
        gp = self.params.get_or_none("group", object)
        assert_or_throw(
            gp is None or isinstance(y, str),
            "when group is set, y must be set as a string",
        )
        self.params.get("height", 0.5)
        width = self.params.get("width", 1.0)
        assert_or_throw(width in [0.5, 1.0], ValueError())

    def process(self, dfs: DataFrames) -> None:
        kwargs: Dict[str, Any] = {
            k: v
            for k, v in self.params.items()
            if k
            not in ["top_n", "order_by", "x", "y", "kind", "width", "height", "group"]
        }
        top_n = self.params.get("top_n", 0)
        df = self._select_top(dfs[0], top_n).as_pandas()
        if "order_by" in self.params:
            order_by: Any = parse_presort_exp(
                self.params.get_or_throw("order_by", object)
            )
        else:
            order_by = self.partition_spec.presort
        self._plot(
            df,
            self.partition_spec.partition_by,
            x=self.params.get_or_throw("x", str),
            y=self.params.get_or_none("y", object),
            kind=self.params.get("kind", self.kind),
            width=self.params.get("width", 1.0),
            height=self.params.get("height", 0.5),
            order_by=order_by,
            group=self.params.get_or_none("group", object),
            **kwargs,
        )

    def _select_top(self, df: DataFrame, top_n: int):
        if top_n > 0:
            if len(self.partition_spec.partition_by) > 0:
                p_keys = ", ".join(self.partition_spec.partition_by)
                if len(self.partition_spec.presort) > 0:
                    sort_expr = f"ORDER BY {self.partition_spec.presort_expr}"
                else:
                    sort_expr = ""
                cols = ", ".join(df.schema.names)
                sql = """
                SELECT {cols} FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY {p_keys} {sort_expr})
                                                AS __top_row_number__
                    FROM __plot_df__) WHERE __top_row_number__ <= {top_n}
                """.format(
                    cols=cols, p_keys=p_keys, sort_expr=sort_expr, top_n=top_n
                )
                df = self.execution_engine.default_sql_engine.select(
                    DataFrames(__plot_df__=df), sql
                )
            else:
                order_expr = ""
                if "order_by" in self.params:
                    order_by = parse_presort_exp(
                        self.params.get_or_throw("order_by", object)
                    )
                    if len(order_by) > 0:
                        order_expr = "ORDER BY " + ", ".join(
                            k + " " + ("ASC" if v else "DESC")
                            for k, v in order_by.items()
                        )
                sql = """
                SELECT * FROM __plot_df__ {order_expr} LIMIT {top_n}
                """.format(
                    order_expr=order_expr, top_n=top_n
                )
                df = self.execution_engine.default_sql_engine.select(
                    DataFrames(__plot_df__=df), sql
                )
        return df

    def _plot(
        self,
        df: pd.DataFrame,
        figure: List[str],
        x: Any,
        y: Any,
        kind: str,
        width: float,
        height: float,
        order_by: Dict[str, bool],
        group: Any,
        **kwargs,
    ) -> None:
        if len(figure) > 0:
            groups = df.groupby(figure)
            fig, specs = _create_fig(groups.ngroups, width=width, height=height)
            i = 0
            for title, gp in df.groupby(figure):
                sub = gp.drop(figure, axis=1)
                sub = self._sort_sub(sub, x, order_by)
                ax = fig.add_subplot(specs[i])
                self._plot_sub(
                    sub=sub,
                    group=group,
                    x=x,
                    y=y,
                    kind=kind,
                    ax=ax,
                    title=str(title),
                    **kwargs,
                )
                i += 1
        else:
            fig, specs = _create_fig(1, width=width, height=height)
            ax = fig.add_subplot(specs[0])
            sub = self._sort_sub(df, x, order_by)
            self._plot_sub(sub=sub, group=group, x=x, y=y, kind=kind, ax=ax, **kwargs)

    def _plot_sub(
        self,
        sub: pd.DataFrame,
        group: Any,
        x: Any,
        y: Any,
        kind: str,
        ax: Any,
        **kwargs: Any,
    ):
        if group is None:
            sub.plot(x=x, y=y, kind=kind, ax=ax, **kwargs)
        else:
            names: List[str] = []
            for name, gp in sub.groupby(group):
                gp.plot(x=x, y=y, kind=kind, ax=ax, **kwargs)
                names.append(str(name))
            ax.legend(names)

    def _sort_sub(
        self, sdf: pd.DataFrame, x: Any, order_by: Dict[str, Any]
    ) -> pd.DataFrame:
        if len(order_by) > 0:
            sdf = sdf.sort_values(
                list(order_by.keys()), ascending=list(order_by.values())
            )
        else:
            sdf = sdf.sort_values(x)
        return sdf


class PlotLine(Plot):
    @property
    def kind(self) -> str:
        return "line"


class PlotBar(Plot):
    @property
    def kind(self) -> str:
        return "bar"


class PlotBarH(Plot):
    @property
    def kind(self) -> str:
        return "barh"

    def _sort_sub(
        self, sdf: pd.DataFrame, x: Any, order_by: Dict[str, Any]
    ) -> pd.DataFrame:
        if len(order_by) > 0:
            sdf = sdf.sort_values(
                list(order_by.keys()), ascending=[~x for x in order_by.values()]
            )
        else:
            sdf = sdf.sort_values(x, ascending=False)
        return sdf


def _create_fig(
    num: int, width: float = 1.0, height: float = 0.5
) -> Tuple[Figure, List[GridSpec]]:
    tuples: List[Any] = []
    if width == 0.5:
        cols = 2
        rows = math.ceil(num / 2)
        x, y = 0, 0
        for _ in range(num):
            tuples.append([y, x])
            x += 1
            if x == 2:
                x = 0
                y += 1
    elif width == 1:
        cols = 1
        rows = num
        for i in range(num):
            tuples.append([i, 0])
    else:  # pragma: no cover
        raise ValueError(width)
    figsize = (12, 12 * height * rows)
    fig = plt.figure(figsize=figsize, constrained_layout=True)
    spec = GridSpec(ncols=cols, nrows=rows, figure=fig)
    return fig, [spec[t[0], t[1]] for t in tuples]
