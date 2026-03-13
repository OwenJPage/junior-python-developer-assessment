from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI

data = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    data["connection"] = await psycopg.AsyncConnection.connect(
        host="postgres",
        port=5432,
        user="uniofsheffield",
        password="jessop",
        dbname="uniofsheffield",
    )

    yield

    await data["connection"].close()


app = FastAPI(lifespan=lifespan)


@app.get("/customer/{id}")
async def customer(id):
    pass
