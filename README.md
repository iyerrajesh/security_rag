# Security Event RAG — Portable (Docker Compose)

A self-contained version of the security-event RAG pipeline. No AWS, no auth.
Runs on any VM with Docker: a pgvector Postgres and a FastAPI service.

- **Vector store**: pgvector/pgvector:pg16 (Postgres 16, pgvector >= 0.8), data in a named volume
- **Embeddings**: Voyage (`voyage-3`, 1024-dim)
- **Completion / query-parsing**: Anthropic (`claude-sonnet-4-6` / Haiku)
- **API**: FastAPI (`/query`, `/events`, `/events/batch`, `/health`)

## Architecture

```
POST /events ──► FastAPI (api) ──► Voyage embed ──► INSERT ──► Postgres+pgvector (db)
POST /query  ──► FastAPI (api) ──► parse (Haiku) ──► hybrid retrieve (SQL filter + HNSW)
                                                  └► answer (Claude Sonnet)
```

The schema is created automatically on API startup (`migrate.py`), replacing
the old CDK custom resource.

## Layout

```
security_rag_portable/
├── app/
│   ├── main.py            # FastAPI routes + startup migration
│   ├── providers.py       # Anthropic + Voyage, behind a swappable interface
│   ├── rag.py             # embed / hybrid retrieve / answer / ingest
│   ├── query_parser.py    # Haiku -> filters, deterministic relative time
│   ├── migrate.py         # CREATE EXTENSION + schema (idempotent)
│   ├── db.py              # psycopg3 connection pool
│   └── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

## Run

```bash
cp .env.example .env
# edit .env: set ANTHROPIC_API_KEY and VOYAGE_API_KEY (and a real DB password)

docker compose up -d --build
curl localhost:8000/health
```

## Use

Ingest an event:
```bash
curl -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "detection_type": "loitering",
  "detected_at": "2026-06-20T23:14:00Z",
  "camera_id": "cam-07",
  "zone": "loading_dock",
  "confidence": 0.91,
  "narration": "A person remained near the dock door for over 4 minutes.",
  "metadata": {"track_id": "t-88"}
}'
```

Query:
```bash
curl -X POST localhost:8000/query -H 'Content-Type: application/json' -d '{
  "question": "Show me loitering near the loading dock last weekend"
}'
```

## Notes
- **Switching models**: change `EMBED_MODEL` / `ANSWER_MODEL` / `PARSE_MODEL`
  in `.env`. If you change the embedding model, keep `EMBED_DIMS` and the
  `VECTOR(1024)` column in `migrate.py` in sync (re-embedding existing rows
  is required if dims change).
- **Other providers**: `providers.py` defines `EmbeddingProvider` /
  `CompletionProvider` interfaces — add an OpenAI/self-hosted class and select
  it there. The rest of the pipeline is unchanged.
- **DB durability**: data lives in the `pgdata` volume. Back it up with
  `docker compose exec db pg_dump ...`.
- **DB is internal** to the compose network by default; uncomment the `ports`
  block in `docker-compose.yml` to reach it from the VM host.
- **Scale**: for large event volumes, partition `security_events` by month on
  `detected_at`.
```
