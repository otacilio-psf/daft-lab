from pipelines.utils import catalog, read_table, write_table, renaming_snake_case
from daft import col, lit, DataType

catalog = catalog("silver")

class TitleBasicsSilverPipeline:

    def read_table(self):
        self._df = read_table(catalog, "bronze.title_basics")
        return self

    def fix_table(self):
    
        df_bad_data = self._df.where(col("genres").is_null())
        
        df_fixed = (
            df_bad_data
            .with_column("genres", col('runtimeMinutes'))
            .with_column("runtimeMinutes", col('endYear'))
            .with_column("endYear", col('startYear'))
            .with_column("startYear", col('isAdult').cast(DataType.string()))
            .with_column("isAdult", col('originalTitle').cast(DataType.int64()))
            .with_column("originalTitle", col('primaryTitle').str.split("\t").list.get(1))
            .with_column("primaryTitle", col('primaryTitle').str.split("\t").list.get(0))
        )

        self._df =  self._df.where(~col("genres").is_null()).concat(df_fixed)

        return self
    
    def renaming_table(self):
        self._df = renaming_snake_case(self._df)
        return self
    
    def is_adult_flag(self):
        self._df = self._df.with_column("is_adult_flag", (col("is_adult")==0).if_else(lit("N"), lit("Y")))
        return self

    def fix_data_types(self):
        self._df = (
            self._df
            .with_column("average_rating", col("average_rating").cast(DataType.int16()))
            .with_column("start_year", col("start_year").cast(DataType.int16()))
            .with_column("end_year", col("end_year").cast(DataType.int16()))
            .with_column("runtime_minutes", col("runtime_minutes").cast(DataType.int16()))
        )
        return self

    def genre_logic(self):
        self._df = (
            self._df
            .with_column("genres", (col("genres")==r'\N').if_else(lit("Unknown"), col("genres")))
            .with_column("main_genre", col('genres').str.split(",").list.get(0))
        )
        return self
    
    def select_cols(self):
        self._df = self._df.select(
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "is_adult_flag",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "main_genre",
            "load_datetime"
        )
        return self
    
    def write_silver(self):
        write_table(catalog, "silver.title_basics", self._df)

    def show(self):
        self._df.show()


if __name__ == "__main__":

    (
        TitleBasicsSilverPipeline()
        .read_table()
        .fix_table()
        .renaming_table()
        .is_adult_flag()
        .fix_data_types()
        .genre_logic()
        .select_cols()
        .show()
    )