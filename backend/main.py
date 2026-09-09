from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router
from pipeline.db import ensure_databases

app = FastAPI(title="Sonique Backend")

# ✅ CORS setup
origins: list[str] = [
    "http://localhost:19006",
    "http://127.0.0.1:19006",
    "http://localhost:8081",
    "http://127.0.0.1:8081",
    "http://localhost:8082",
    "http://127.0.0.1:8082",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # or ["*"] for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    ensure_databases()


# Include your routes
app.include_router(router)


@app.get("/")  # type: ignore[untyped-decorator]
async def root() -> dict[str, Any]:
    return {"message": "Sonique online"}