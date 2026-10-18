"""    python -m lakehouse bootstrap [--reset]      create namespaces and tables
    python -m lakehouse sink raw|validated|quarantine [...]
    python -m lakehouse curate [TYPE ...]        validated -> curated (drops repeats)
    python -m lakehouse status                   rows, files and snapshots per table
    python -m lakehouse compact [TABLE ...] [--benchmark]
    python -m lakehouse expire|orphans [TABLE ...] --older-than 1h
    python -m lakehouse demo evolve|timetravel|retention|all
    python -m lakehouse facts --output facts.json    contents summary, read by e2e.sh
"""

import sys

COMMANDS = ("bootstrap", "sink", "curate", "status", "compact", "expire", "orphans", "demo", "facts")


def main(argv: list[str]) -> int:
    if not argv or argv[0] not in COMMANDS:
        print(__doc__, file=sys.stderr)
        return 2
    command, rest = argv[0], argv[1:]
    if command == "sink":
        from lakehouse import sink
        return sink.main(rest)

    from lakehouse.catalog import get_catalog
    catalog = get_catalog()
    if command == "bootstrap":
        from lakehouse import bootstrap
        return bootstrap.main(rest, catalog)
    if command == "curate":
        from lakehouse import curate
        return curate.main(rest, catalog)
    if command in ("compact", "expire", "orphans"):
        from lakehouse import maintenance
        return maintenance.main(command, rest, catalog)
    if command == "demo":
        from lakehouse import demos
        return demos.main(rest, catalog)
    if command == "facts":
        from lakehouse import facts
        return facts.main(rest, catalog)
    from lakehouse import status
    return status.main(catalog)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
