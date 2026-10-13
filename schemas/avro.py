"""Generate Avro schemas from the Pydantic event models.

The Pydantic models stay the single source of truth; the Avro schemas in
`schemas/avro/` are generated from them and committed, one file per event type
and version, so the registry contract is reviewable in diffs.

    python -m schemas.avro           # regenerate schemas/avro/*.avsc
    python -m schemas.avro --check   # exit 1 if the committed files are stale

Avro can't express value ranges or string patterns (confidence 0..1, `V\\d+`);
the validator enforces those with the Pydantic models.
"""

import json
import sys
import typing
from datetime import datetime
from enum import Enum
from pathlib import Path
from uuid import UUID

from pydantic import AwareDatetime
from pydantic_core import PydanticUndefined

from .base import BaseEvent
from .events import TOPICS

NAMESPACE = "av.fleet"
AVRO_DIR = Path(__file__).parent / "avro"
MODELS: list[type[BaseEvent]] = list(TOPICS)


def event_type_of(model: type[BaseEvent]) -> str:
    return model.model_fields["event_type"].default


def _avro_type(annotation, named: set[str]):
    origin = typing.get_origin(annotation)
    if origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) != 1:
            raise TypeError(f"unsupported union {annotation}")
        return ["null", _avro_type(args[0], named)]
    if origin is typing.Literal:
        return "string"
    if annotation in (AwareDatetime, datetime):
        return {"type": "long", "logicalType": "timestamp-millis"}
    if annotation is UUID:
        return {"type": "string", "logicalType": "uuid"}
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        full_name = f"{NAMESPACE}.{annotation.__name__}"
        if full_name in named:  # Avro named types are defined once, then referenced
            return full_name
        named.add(full_name)
        schema = {"type": "enum", "name": annotation.__name__, "namespace": NAMESPACE,
                  "symbols": [m.value for m in annotation]}
        if "unknown" in schema["symbols"]:
            schema["default"] = "unknown"  # readers map symbols they don't know to "unknown"
        return schema
    simple = {str: "string", int: "int", float: "double", bool: "boolean"}
    if annotation in simple:
        return simple[annotation]
    raise TypeError(f"no Avro mapping for {annotation!r}")


def avro_schema(model: type[BaseEvent], version: int) -> dict:
    named: set[str] = set()
    fields = []
    for name in model.fields_for_version(version):
        info = model.model_fields[name]
        field = {"name": name, "type": _avro_type(info.annotation, named)}
        if info.description:
            field["doc"] = info.description
        if name == "schema_version":
            field["default"] = version
        elif info.default is not PydanticUndefined and info.default_factory is None:
            default = info.default
            field["default"] = default.value if isinstance(default, Enum) else default
        fields.append(field)
    return {
        "type": "record",
        "name": model.__name__,
        "namespace": NAMESPACE,
        "doc": f"{(model.__doc__ or '').strip().splitlines()[0]} Schema version {version}.",
        "fields": fields,
    }


def all_schemas() -> dict[str, dict]:
    """{"perception.v2.avsc": schema, ...} for every event type and version."""
    return {
        f"{event_type_of(m)}.v{v}.avsc": avro_schema(m, v)
        for m in MODELS for v in range(1, m.latest_version() + 1)
    }


def to_avro_record(event: BaseEvent) -> dict:
    """Event as a dict for Avro serialization with the latest schema of its type."""
    return {k: (v.value if isinstance(v, Enum) else v) for k, v in event.model_dump().items()}


def load(event_type: str, version: int) -> str:
    return (AVRO_DIR / f"{event_type}.v{version}.avsc").read_text()


def main(argv=None) -> int:
    check = "--check" in (argv if argv is not None else sys.argv[1:])
    AVRO_DIR.mkdir(exist_ok=True)
    stale = []
    for filename, schema in all_schemas().items():
        text = json.dumps(schema, indent=2) + "\n"
        path = AVRO_DIR / filename
        if check:
            if not path.exists() or path.read_text() != text:
                stale.append(filename)
        else:
            path.write_text(text)
            print(f"wrote {path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path}")
    if stale:
        print(f"stale Avro schemas (run `python -m schemas.avro`): {', '.join(stale)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
