import re
from typing import Callable, Optional, Tuple
from triad.utils.assertion import assert_or_throw

SQLITE_TABLE_REGEX = re.compile(r"([0-9a-zA-Z\-_]+\.sqlite3?)\.([0-9a-zA-Z\-_]+)")


def transform_sqlite_sql(
    sql: str, validate: Callable[[str], None]
) -> Tuple[str, Optional[str]]:
    tables = {m.group(1) for m in SQLITE_TABLE_REGEX.finditer(sql)}
    assert_or_throw(
        len(tables) <= 1,
        NotImplementedError("can't have multiple sources in one statement", tables),
    )
    if len(tables) > 0:
        validate(list(tables)[0])
    return (
        SQLITE_TABLE_REGEX.sub(r"\2", sql),
        None if len(tables) == 0 else list(tables)[0],
    )
