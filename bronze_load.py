from datetime import datetime
from daft import lit
import daft
from pipelines.utils import catalog, write_table

catalog = catalog("bronze")

def bronze_ingestion(table):
    table_raw = table.replace("_", ".")
    df = (
        daft
        .read_csv(f"data/raw/{table_raw}.tsv", allow_variable_columns=True, delimiter="\t")
        .with_column("load_datetime", lit(datetime.now()))
    )

    write_table(catalog, f"bronze.{table}", df)

if __name__ == "__main__":
    for t in ["title_basics", "title_ratings"]:
        bronze_ingestion(t)