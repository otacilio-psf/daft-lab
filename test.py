from pipelines.utils import catalog

catalog = catalog("silver")

table = catalog.load_table("silver.title_basics")

print(table.schema())