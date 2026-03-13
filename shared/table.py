from typing import Any, Iterable, NotRequired, TypedDict

import psycopg
from psycopg import sql


class FieldDescription(TypedDict):
    pg_type: str
    extra: NotRequired[str]


class Table:
    def __init__(
        self,
        table_name: str,
        fields: dict[str, str | FieldDescription],
        extra: Iterable[str] | None = None,
    ):
        self._conn: psycopg.AsyncConnection | None = None

        self._table_name = table_name

        self._fields = tuple(fields.keys())
        self._fields_set = set(fields.keys())

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
            sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields}, {extra});").format(
                name=sql.Identifier(table_name),
                fields=sql.SQL(", ").join(fields_sql),
                extra=sql.SQL(", ").join(map(sql.SQL, extra)),
            )
            if extra is not None
            else sql.SQL("CREATE TABLE IF NOT EXISTS {name} ({fields});").format(
                name=sql.Identifier(table_name), fields=sql.SQL(", ").join(fields_sql)
            )
        )

    async def bind(self, connection: psycopg.AsyncConnection):

        self._conn = connection
        # TODO: Potentially move this out of here
        await connection.set_autocommit(True)

        async with connection.cursor() as cur:
            # TODO: Handle errors and return
            await cur.execute(self._table_sql)

    async def is_empty(self) -> bool | RuntimeError:
        if self._conn is None:
            return RuntimeError(
                "Attempting to operate on table before it has been initialised. Call bind() to bind to a connection and initialise the table."
            )

        statement = sql.SQL("SELECT NOT EXISTS (SELECT 1 FROM {name});").format(
            name=sql.Identifier(self._table_name)
        )

        async with self._conn.cursor() as cur:
            await cur.execute(statement)

            (result,) = await cur.fetchone()

            return result

    async def insert(
        self, row_dict: dict[str, Any], return_fields: Iterable[str] | None = None
    ) -> tuple | None | KeyError:

        row_dict_diff = row_dict.keys() - self._fields_set
        if len(row_dict_diff) > 0:
            return KeyError(
                f"Field {tuple(row_dict_diff)[0]} (from row_dict) is not a valid field within {self._table_name}."
                if len(row_dict_diff) == 1
                else f"Fields {', '.join(row_dict_diff)} (from row_dict) are not valid fields within {self._table_name}."
            )

        ret_field_diff = (
            set(return_fields) - self._fields_set if return_fields is not None else []
        )
        if len(ret_field_diff) > 0:
            return KeyError(
                f"Field {tuple(ret_field_diff)[0]} (from return_fields) is not a valid field within {self._table_name}."
                if len(ret_field_diff) == 1
                else f"Fields {', '.join(ret_field_diff)} (from return_fields) are not valid fields within {self._table_name}."
            )

        fields = (sql.Identifier(f) for f in row_dict.keys())
        values = (sql.Literal(v) for v in row_dict.values())
        return_fields = (
            (sql.Identifier(f) for f in return_fields)
            if return_fields is not None
            else (sql.Literal(None),)
        )

        statement = sql.SQL(
            "INSERT INTO {name} ({fields}) VALUES ({values}) RETURNING ({return_fields});"
        ).format(
            name=sql.Identifier(self._table_name),
            fields=sql.SQL(", ").join(fields),
            values=sql.SQL(", ").join(values),
            return_fields=sql.SQL(", ").join(return_fields),
        )

        async with self._conn.cursor() as cur:
            # TODO: Handle errors from inserting
            await cur.execute(statement)

            return await cur.fetchone() if return_fields is not None else None

    async def get_row(self, **kwargs) -> dict | KeyError:
        field_diff = kwargs.keys() - self._fields_set
        if len(field_diff) > 0:
            return KeyError(
                f"Field {tuple(field_diff)[0]} is not a valid field within {self._table_name}."
                if len(field_diff) == 1
                else f"Fields {', '.join(field_diff)} are not valid fields within {self._table_name}."
            )

        where_exprs = (
            sql.SQL("{field} = {value}").format(
                field=sql.Identifier(f), value=sql.Literal(v)
            )
            for f, v in kwargs.items()
        )

        statement = sql.SQL(
            "SELECT * FROM {name} WHERE ({where_exprs}) LIMIT 1;"
        ).format(
            name=sql.Identifier(self._table_name),
            where_exprs=sql.SQL(", ").join(where_exprs),
        )

        async with self._conn.cursor() as cur:
            # TODO: Handle exceptions and return
            await cur.execute(statement)

            result = await cur.fetchone()

            return {k: v for k, v in zip(self._fields, result)}

    async def select(
        self,
        fields: Iterable[str | sql.SQL | sql.Composed],
        where: sql.SQL | sql.Composed | None = None,
        group_by: Iterable[str] | None = None,
        having: sql.SQL | sql.Composed | None = None,
        order_by: str | sql.SQL | sql.Composed | None = None,
        limit: int | None = None,
    ):

        pass

        # str_field_diff = set(
        #     f for f in fields if isinstance(f, str)
        # ) - self._fields_set.union(("*",))
        # if len(str_field_diff) > 0:
        #     f"Field {} "
