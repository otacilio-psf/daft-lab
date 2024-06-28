#### Business Rules
#
#- title_basics:
#  - columns names should be all in [snake_case](https://en.wikipedia.org/wiki/Snake_case)
#  - column is_adult need to be a flag Y/N, null values or diff them 0 or 1 should be Y (better yes then n)
#  - column is_adult renamed for is_adult_flag
#  - columns start_year and end_year need to be integer
#  - column runtime_minutes need to be integer
#  - change '\N' from genres to 'Unknown'
#  - extract main_genre need to be the first genre of genres
#  
#- title_ratings :
#  - columns names should be all in [snake_case](https://en.wikipedia.org/wiki/Snake_case)
#  - column average_rating need to be float
#  - column num_votes need to be integer
#  - create num_votes_type
#    - 0 <= num_votes <= 500 - unexplored
#    - 500 < num_votes <= 2500 - known
#    - 2500 < num_votes - trending
#

from pipelines.title_basic_pipeline import TitleBasicsSilverPipeline
from pipelines.title_ratings_pipeline import TitleRatingsSilverPipeline


if __name__ == "__main__":
    print("LOADING: Title Basics")
    (
        TitleBasicsSilverPipeline()
        .read_table()
        .fix_table()
        .renaming_table()
        .is_adult_flag()
        .fix_data_types()
        .genre_logic()
        .select_cols()
        .write_silver()
    )

    print("LOADING: Title Ratings")
    (
        TitleRatingsSilverPipeline()
        .read_table()
        .renaming_table()
        .fix_data_types()
        .num_votes_type_logic()
        .select_cols()
        .write_silver()
    )