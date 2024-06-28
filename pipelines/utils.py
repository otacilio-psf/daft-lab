from pyiceberg.catalog.sql import SqlCatalog
from daft import col
import re

def catalog(*args:str):
    warehouse_path = "data"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": f"file://{warehouse_path}",
        }
    )

    for ns in args:
        if not (ns,) in catalog.list_namespaces():
            catalog.create_namespace(ns)
    
    return catalog

def write_table(catalog, table, df, mode="overwrite"):
    table_tuple = tuple(table.split("."))
    if not table_tuple in catalog.list_tables(table_tuple[0]):
        iceberg_table = catalog.create_table(
            table,
            schema=df.to_arrow().schema
        )
    else:
        iceberg_table = catalog.load_table(table)
    
    df.write_iceberg(iceberg_table, mode)

read_table = lambda catalog, table: catalog.load_table(table).to_daft()

def renaming_snake_case(df):
    to_snake = lambda c: re.sub('(.)([A-Z][a-z]+)', r'\1_\2', c).lower()
    column_renaming = [col(c).alias(to_snake(c)) for c in df.column_names]
    return df.select(*column_renaming)