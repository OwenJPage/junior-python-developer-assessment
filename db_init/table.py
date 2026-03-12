from typing import Any, Callable, Iterable, NotRequired, Optional, TypedDict

from psycopg import sql


class FieldDescription(TypedDict):
    pg_type: str
    becomes: NotRequired[Callable[[Any], Any]]
    extra: NotRequired[str]


test_dict: dict[str, str | FieldDescription] = {
    "id": {"type": "serial", "becomes": "int", "extra": "PRIMARY KEY"},
    "first_name": "text",
}


class TestTable:
    def __init__(
        self,
        name: str,
        fields: dict[str, str | FieldDescription],
        extra: Iterable[str] | None = None,
    ):

        if extra is not None:
            sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields}, {extra})")
        else:
            sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields})")
