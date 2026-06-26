"""FastAPI service exposing the RAG pipeline. No auth (per request).
Collapses the former Lambda handlers into routes; runs schema migration
on startup in place of the CDK custom resource."""
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any

from migrate import run_migrations
from query_parser import parse_query
from rag import answer, ingest_event


@asynccontextmanager
async def lifespan(app: FastAPI):
    run_migrations()
    yield


app = FastAPI(title="Security Event RAG", lifespan=lifespan)


class QueryRequest(BaseModel):
    question: str


class EventRequest(BaseModel):
    detection_type: str
    detected_at: str
    camera_id: str
    zone: str | None = None
    confidence: float | None = None
    frame_paths: list[str] | None = None
    narration: str | None = None
    metadata: dict[str, Any] | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query")
def query(req: QueryRequest):
    f = parse_query(req.question)
    result = answer(
        f.get("semantic_query") or req.question,
        detection_type=f.get("detection_type"),
        start=f.get("start"),
        end=f.get("end"),
        camera_id=f.get("camera_id"),
        zone=f.get("zone"),
    )
    result["filters"] = f
    return result


@app.post("/events", status_code=201)
def create_event(req: EventRequest):
    try:
        eid = ingest_event(req.model_dump(exclude_none=True))
        return {"ingested": 1, "event_id": eid}
    except Exception as e:
        print(f"ingest error: {e}")
        raise HTTPException(status_code=500, detail="ingest failed")


@app.post("/events/batch", status_code=201)
def create_events(reqs: list[EventRequest]):
    ids = [ingest_event(r.model_dump(exclude_none=True)) for r in reqs]
    return {"ingested": len(ids), "event_ids": ids}
