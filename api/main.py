from fastapi import FastAPI

app = FastAPI()


@app.get("/customer/{id}")
async def customer(id):
    pass
