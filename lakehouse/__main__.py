"""    python -m lakehouse bootstrap [--reset]   create namespaces and tables
    python -m lakehouse sink raw|validated|quarantine [...]
    python -m lakehouse status                  rows, files and snapshots per table
"""

import sys

COMMANDS = ("bootstrap", "sink", "status")


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
    from lakehouse import status
    return status.main(catalog)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
