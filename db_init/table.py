from typing import Any, Callable, Iterable, NotRequired, TypedDict

from psycopg import sql


class FieldDescription(TypedDict):
    pg_type: str
    becomes: NotRequired[Callable[[Any], Any]]
    extra: NotRequired[str]


class Table:
    def __init__(
        self,
        table_name: str,
        fields: dict[str, str | FieldDescription],
        extra: Iterable[str] | None = None,
    ):
        self._fields = set(fields.keys())
        self._converters = {
            f: d["becomes"]
            for f, d in fields.items()
            if isinstance(d, dict) and "becomes" in d
        }

        fields_sql = [
            sql.SQL("{f_name} {f_type} {extra}").format(
                f_name=sql.Identifier(f),
                f_type=sql.SQL(d["pg_type"]),
                extra=sql.SQL(d["extra"]),
            )
            if isinstance(d, dict) and "extra" in d
            else sql.SQL("{f_name} {f_type}").format(
                f_name=sql.Identifier(f),
                f_type=sql.SQL(d["pg_type"] if isinstance(d, dict) else d),
            )
            for f, d in fields.items()
        ]

        self._table_sql = (
            sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields}, {extra})").format(
                name=sql.Identifier(table_name),
                fields=sql.SQL(", ").join(fields_sql),
                extra=sql.SQL(", ").join(map(sql.SQL, extra)),
            )
            if extra is not None
            else sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields})").format(
                name=sql.Identifier(table_name), fields=sql.SQL(", ").join(fields_sql)
            )
        )

        self._insert_sql = sql.SQL("INSERT INTO {name} ({fields}) VALUES ({values})")

    def insert(self, row: dict):
        pass


if __name__ == "__main__":
    table = Table(
        "my_table",
        {
            "id": {"pg_type": "serial", "extra": "PRIMARY KEY"},
            "name": "text",
            "legs": "int",
        },
        extra=("CONSTRAINT fk_urmum FOREIGN KEY(legs) REFERENCES Customers(id)",),
    )

    print(table._table_sql.as_string())
