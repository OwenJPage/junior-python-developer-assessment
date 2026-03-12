import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

import psycopg


class CustomerStatus(Enum):
    active = "active"
    archived = "archived"
    suspended = "suspended"


@dataclass
class Customer:
    id: int
    first_name: str
    last_name: str
    email: str
    status: CustomerStatus

    def get_row(
        self, include_id=False
    ) -> tuple[str, str, str, str] | tuple[int, str, str, str, str]:
        if include_id:
            return (
                self.id,
                self.first_name,
                self.last_name,
                self.email,
                self.status.value,
            )
        else:
            return self.first_name, self.last_name, self.email, self.status.value


Customer(1, "", "", "", CustomerStatus("active"))


class CustomerTable:
    def __init__(self, connection: psycopg.AsyncConnection):
        self._logger = logging.getLogger("customer_table")
        self._conn = connection

    async def initialise(self):
        self._logger.info("Initialising customer table")

        async with self._conn.cursor() as cur:
            try:
                self._logger.info("Creating customer_status enum type")
                await cur.execute(
                    "CREATE TYPE customer_status AS ENUM ('active', 'suspended', 'archived');"
                )
            except psycopg.errors.DuplicateObject:
                self._logger.warning("Enum customer_status already exists, skipping")

            self._logger.debug("Committing")
            await self._conn.commit()

        async with self._conn.cursor() as cur:
            # Create the table ONLY if it does not already exist
            self._logger.info("Creating customers table (if not already existing)")
            await cur.execute("""
                              CREATE TABLE IF NOT EXISTS Customers
                              (
                                  id         serial PRIMARY KEY,
                                  first_name text,
                                  last_name  text,
                                  email      text,
                                  status     customer_status
                              );
                              """)

    async def insert(self, customer: Customer):
        self._logger.debug("Call to insert")

        # TODO: Handle errors by returning

        async with self._conn.cursor() as cur:
            self._logger.info("Inserting new row into customers table")
            await cur.execute(
                "INSERT INTO Customers (first_name, last_name, email, status) VALUES (%s, %s, %s, %s)",
                (
                    customer.first_name,
                    customer.last_name,
                    customer.email,
                    customer.status.value,
                ),
            )

            self._logger.debug("Committing")
            await self._conn.commit()

    async def insert_many(
        self, customers: Iterable[Customer], return_rows=False
    ) -> tuple[Customer] | None:
        self._logger.debug("Call to insert_many")

        async with self._conn.cursor() as cur:
            self._logger.info("Initiating COPY operation")
            async with cur.copy(
                "COPY Customers (first_name, last_name, email, status) FROM STDIN"
            ) as copy:
                for customer in customers:
                    self._logger.debug("Writing row")
                    copy.write_row(customer)
            self._logger.info("Completed copy operation")

            self._logger.debug("Comitting")
            await self._conn.commit()

            # TODO: Return inserted rows when return_rows==True

    async def select(
        self,
        fields: Iterable[str] = ("*",),
        where: dict | None = None,
        group_by: str | None = None,
        limit: int | None = None,
    ):
        pass

    async def delete(
        self,
    ):
        pass


async def main():
    logger = logging.getLogger("async_thread")
    logger.debug("Started async runtime")

    logger.info("Connecting to PostgreSQL")
    async with await psycopg.AsyncConnection.connect(
        host="postgres",
        port=5432,
        user="uniofsheffield",
        password="jessop",
        dbname="uniofsheffield",
    ) as conn:
        logger.info("Connected to PostgreSQL")

        customer_table = CustomerTable(conn)

        await customer_table.initialise()

        new_customer = Customer(
            None,
            "Jane",
            "Austen",
            "janeausten@waterstones.co.uk",
            CustomerStatus("active"),
        )

        await customer_table.insert(new_customer)


# def main():
#     conn: psycopg.Connection
#     with psycopg.connect("user=uniofsheffield password=jessop") as conn:
#         with conn.cursor() as cur:
#             try:
#                 cur.execute(
#                     "CREATE TYPE customer_status AS ENUM ('active', 'suspended', 'archived');"
#                 )
#             except psycopg.errors.DuplicateObject:
#                 pass
#         conn.commit()
#
#         with conn.cursor() as cur:
#             # Create the table ONLY if it does not already exist
#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS Customers (
#                     id serial PRIMARY KEY,
#                     first_name text,
#                     last_name text,
#                     email text,
#                     status customer_status
#                 );
#             """)
#
#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS Orders (
#                     id serial PRIMARY KEY,
#                     customer_id int NOT NULL REFERENCES Customers,
#                     product_name text,
#                     quantity int CHECK (quantity >= 0),
#                     unit_price numeric CHECK (unit_price >= 0)
#                 )
#             """)
#
#             # Check if the table is empty
#             cur.execute("SELECT NOT EXISTS (SELECT id FROM Customers);")
#
#             needs_populating = cur.fetchone()[0]
#
#             if needs_populating:
#                 print("Populating")
#
#         conn.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    logging.info("Starting DB initialisation")

    # Don't need to use this loop when deploying on docker
    asyncio.run(main())
