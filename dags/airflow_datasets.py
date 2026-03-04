from airflow.datasets import Dataset


TITLE_BASICS_DATASET = Dataset("imdb://postgres_movies/public/title_basics")
NAME_BASICS_DATASET = Dataset("imdb://postgres_movies/public/name_basics")
TITLE_CREW_DATASET = Dataset("imdb://postgres_movies/public/title_crew")
TITLE_PRINCIPALS_DATASET = Dataset("imdb://postgres_movies/public/title_principals")
TITLE_RATINGS_DATASET = Dataset("imdb://postgres_movies/public/title_ratings")
