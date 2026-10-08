from fastapi import FastAPI

app = FastAPI(title="Return-Flow")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
