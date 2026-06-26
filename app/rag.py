"""Core RAG: embedding, hybrid retrieval (SQL prefilter + HNSW), answer generation.
Postgres access via psycopg3 pool; models via the provider abstraction."""
from db import get_pool
from providers import embedding_provider, completion_provider


def embed(text: str) -> list[float]:
    return embedding_provider().embed(text)


def _vec_literal(vec: list[float]) -> str:
    # pgvector accepts a bracketed string literal cast to ::vector
    return "[" + ",".join(str(x) for x in vec) + "]"


def retrieve(query: str, detection_type=None, start=None, end=None,
             camera_id=None, zone=None, k=8):
    qvec = _vec_literal(embed(query))
    sql = """
        SELECT event_id, detection_type, detected_at, camera_id, zone,
               narration, metadata,
               1 - (embedding <=> %s::vector) AS similarity
        FROM security_events
        WHERE TRUE
    """
    params: list = [qvec]
    if detection_type:
        sql += " AND detection_type = %s"; params.append(detection_type)
    if start:
        sql += " AND detected_at >= %s";   params.append(start)
    if end:
        sql += " AND detected_at <= %s";   params.append(end)
    if camera_id:
        sql += " AND camera_id = %s";      params.append(camera_id)
    if zone:
        sql += " AND zone = %s";           params.append(zone)
    sql += " ORDER BY embedding <=> %s::vector LIMIT %s"
    params += [qvec, k]

    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def answer(query: str, **filters) -> dict:
    rows = retrieve(query, **filters)
    if not rows:
        return {"answer": "No matching events found for that query.", "events": []}

    context = "\n".join(
        f"[{r[2]}] {r[1]} on camera {r[3]}/zone {r[4]} "
        f"(similarity={r[7]:.2f}): {r[5]}"
        for r in rows
    )
    prompt = (
        "You are a security analyst. Using ONLY the events below, answer the "
        "query concisely. Cite event timestamps and cameras. If the events do "
        "not support an answer, say so.\n\n"
        f"EVENTS:\n{context}\n\nQUERY: {query}"
    )
    text = completion_provider().complete(system=None, prompt=prompt, max_tokens=1024)
    events = [
        {"event_id": str(r[0]), "detection_type": r[1],
         "detected_at": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
         "camera_id": r[3], "zone": r[4], "similarity": round(float(r[7]), 3)}
        for r in rows
    ]
    return {"answer": text, "events": events}


def ingest_event(event: dict) -> str:
    doc = (
        f"Detection: {event['detection_type']}. "
        f"Time: {event['detected_at']}. "
        f"Camera: {event['camera_id']}, zone {event.get('zone')}. "
        f"{event.get('narration', '')}"
    )
    vec = _vec_literal(embed(doc))
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security_events
                  (detection_type, detected_at, camera_id, zone, confidence,
                   frame_paths, narration, metadata, embedding)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::vector)
                RETURNING event_id
                """,
                (
                    event["detection_type"], event["detected_at"], event["camera_id"],
                    event.get("zone"), event.get("confidence"),
                    event.get("frame_paths"), event.get("narration"),
                    __import__("json").dumps(event.get("metadata", {})), vec,
                ),
            )
            event_id = cur.fetchone()[0]
        conn.commit()
    return str(event_id)
