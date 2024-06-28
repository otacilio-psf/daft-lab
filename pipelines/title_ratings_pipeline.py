from pipelines.utils import catalog, read_table, write_table, renaming_snake_case
from daft import col, lit, DataType

catalog = catalog("silver")

class TitleRatingsSilverPipeline:

    def read_table(self):
        self._df = read_table(catalog, "bronze.title_ratings")
        return self
    
    def renaming_table(self):
        self._df = renaming_snake_case(self._df)
        return self
    
    def fix_data_types(self):
        self._df = (
            self._df
            .with_column("average_rating", col("average_rating").cast(DataType.float32()))
            .with_column("num_votes", col("num_votes").cast(DataType.int32()))
        )
        return self

    def num_votes_type_logic(self):
        self._df = (
            self._df
            .with_column("num_votes_type", ((col("num_votes")>= 0)&(col("num_votes")<= 500)).if_else(lit("Unexplored"), lit("ERROR")))
            .with_column("num_votes_type", ((col("num_votes")> 500)&(col("num_votes") <= 2500)).if_else(lit("Known"), col("num_votes_type")))
            .with_column("num_votes_type", (col("num_votes")>2500).if_else(lit("Trending"), col("num_votes_type")))
        )
        return self
    
    def select_cols(self):
        self._df = self._df.select(
            "tconst",
            "average_rating",
            "num_votes",
            "num_votes_type",
            "load_datetime"
        )
        return self

    def write_silver(self):
        write_table(catalog, "silver.title_basics", self._df)

    def show(self):
        self._df.show()

if __name__ == "__main__":

    (
        TitleRatingsSilverPipeline()
        .read_table()
        .renaming_table()
        .fix_data_types()
        .num_votes_type_logic()
        .select_cols()
        .show()
    )