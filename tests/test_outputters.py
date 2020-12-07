from fuggle import Plot, PlotLine, PlotBar, PlotBarH
from fugue import FugueWorkflow
from fuggle import KaggleNativeExecutionEngine
from pytest import raises


def test_plot():
    with FugueWorkflow(KaggleNativeExecutionEngine()) as dag:
        df = dag.df([[0, 1, 2], [0, 3, 4], [0, 5, 6], [1, 2, 3]], "a:int,b:int,c:int")
        df.output(Plot, params=dict(kind="line", x="a"))

        df.partition(by=["a"]).output(Plot, params=dict(kind="line", x="b"))
        df.partition(by=["a"], presort="b desc").output(
            Plot, params=dict(kind="line", x="b")
        )

        df.output(Plot, params=dict(kind="line", top_n=2, x="b"))
        df.partition(by=["a"]).output(Plot, params=dict(kind="line", top_n=2, x="b"))
        df.partition(by=["a"], presort="b desc").output(
            Plot, params=dict(kind="line", top_n=2, x="b")
        )
        df.partition(by=["a"], presort="b desc").output(
            Plot, params=dict(kind="line", top_n=2, x="b", order_by="b desc")
        )
        df.output(Plot, params=dict(kind="line", top_n=2, x="b", order_by="b desc"))

        df.partition(by=["a"], presort="c desc").output(
            Plot, params=dict(kind="line", x="b")
        )
        df.partition(by=["a"], presort="b, c desc").output(
            Plot, params=dict(kind="line", x="b")
        )

        df.output(Plot, params=dict(kind="line", x="b", order_by="b desc", width=1.0))
        df.partition(by=["a"], presort="b, c desc").output(
            Plot, params=dict(kind="line", x="b", order_by="b desc", width=0.5)
        )

        # extra kwargs
        df.partition(by=["a"], presort="b, c desc").output(
            Plot, params=dict(kind="line", x="b", order_by="b desc", color="red")
        )

        # compile time errors
        raises(
            KeyError,
            lambda: df.output(Plot, params=dict(x="a", width=2.0)),
        )  # missing kind
        raises(
            ValueError,
            lambda: df.output(Plot, params=dict(kind="line", x="a", width=2.0)),
        )  # bad width
        raises(
            ValueError,
            lambda: df.output(Plot, params=dict(kind="line", x="a", height="x")),
        )  # bad height
        raises(
            KeyError, lambda: df.output(Plot, params=dict(kind="line", top_n=1))
        )  # missing x
        raises(
            ValueError,
            lambda: df.output(Plot, params=dict(kind="line", top_n="x", x="a")),
        )  # bad top_n
        raises(
            SyntaxError,
            lambda: df.output(
                Plot, params=dict(kind="line", x="a", order_by="a descc")
            ),
        )  # bad order by

        # multiple partition keys
        df = dag.df(
            [[0, 10, 1, 2], [0, 10, 3, 4], [0, 12, 5, 6], [1, 13, 2, 3]],
            "a:int,b:int,c:int,d:int",
        )
        df.partition(by=["a", "b"]).output(Plot, params=dict(kind="line", x="c"))

        # multiple groups
        df = dag.df(
            [[0, 10, 1, 2], [0, 10, 3, 4], [0, 12, 5, 6], [1, 13, 2, 3]],
            "a:int,b:int,c:int,d:int",
        )
        df.partition(by=["a"]).output(
            Plot, params=dict(kind="line", x="b", group="c", y="d")
        )


def test_derived_plot():
    with FugueWorkflow(KaggleNativeExecutionEngine()) as dag:
        df = dag.df([[0, 1, 2], [0, 3, 4], [0, 5, 6], [1, 2, 3]], "a:int,b:int,c:int")
        df.output(PlotLine, params=dict(x="a"))
        df.output(PlotBar, params=dict(x="a"))
        df.output(PlotBarH, params=dict(x="a", order_by="b"))
        raises(
            AssertionError, lambda: df.output(PlotLine, params=dict(x="a", kind="line"))
        )
