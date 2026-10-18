"""Create the namespaces and tables. Idempotent: existing tables are left as they are.

    python -m lakehouse bootstrap
    python -m lakehouse bootstrap --reset   # DROP and recreate every table (deletes local lakehouse data)
"""

import sys

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from lakehouse.tables import NAMESPACES, TABLE_PROPERTIES, TableDef, all_tables


def ensure_tables(catalog: Catalog, tables: list[TableDef] = None, reset: bool = False) -> dict[str, str]:
    """Returns {identifier: "created" | "exists" | "recreated" | "drifted"}."""
    outcome = {}
    tables = tables or all_tables()
    for namespace in dict.fromkeys([*NAMESPACES, *(t.namespace for t in tables)]):
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
    for table in tables:
        existed = False
        try:
            existing = catalog.load_table(table.identifier)
            existed = True
        except NoSuchTableError:
            existing = None
        if existing is not None and reset:
            catalog.purge_table(table.identifier)
            existing = None
        if existing is None:
            catalog.create_table(table.identifier, schema=table.schema, partition_spec=table.partition_spec(),
                                 properties=TABLE_PROPERTIES)
            outcome[table.identifier] = "recreated" if existed else "created"
        else:
            # Never alter a live table here: a column change is a deliberate schema evolution.
            have = [f.name for f in existing.schema().fields]
            want = [f.name for f in table.schema.fields]
            outcome[table.identifier] = "exists" if have == want else "drifted"
    return outcome


def main(argv: list[str], catalog: Catalog) -> int:
    outcome = ensure_tables(catalog, reset="--reset" in argv)
    for identifier, state in outcome.items():
        print(f"  {identifier:40} {state}")
    drifted = [i for i, s in outcome.items() if s == "drifted"]
    if drifted:
        print(f"columns differ from the models for: {', '.join(drifted)} (evolve the table or run with --reset)",
              file=sys.stderr)
        return 1
    return 0
