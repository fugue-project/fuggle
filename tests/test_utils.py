from fuggle._utils import transform_sqlite_sql
from pytest import raises
from triad.utils.assertion import assert_or_throw


def test_transform_sqlite_sql():
    def should_not_call(s):
        raise ValueError

    assert ("", None) == transform_sqlite_sql("", should_not_call)
    assert ("x", "a.sqlite") == transform_sqlite_sql(
        "a.sqlite.x", lambda x: assert_or_throw("a.sqlite" == x)
    )
    assert ("b y ", "a.sqlite3") == transform_sqlite_sql(
        "b a.sqlite3.y ", lambda x: assert_or_throw("a.sqlite3" == x)
    )
    assert ("SELECT * FROM p INNER JOIN z", "a.sqlite3") == transform_sqlite_sql(
        "SELECT * FROM a.sqlite3.p INNER JOIN a.sqlite3.z",
        lambda x: assert_or_throw("a.sqlite3" == x),
    )
    # multiple source is not allowed
    with raises(NotImplementedError):
        transform_sqlite_sql(
            "SELECT * FROM a.sqlite.p INNER JOIN a.sqlite3.z", should_not_call
        )
    # validation failed
    with raises(AssertionError):
        transform_sqlite_sql(
            "b a.sqlite3.y ", lambda x: assert_or_throw("a.sqlite" == x)
        )