from airflow.datasets import Dataset


TITLE_BASICS_DATASET = Dataset("imdb://postgres_movies/public/title_basics")
TITLE_RATINGS_DATASET = Dataset("imdb://postgres_movies/public/title_ratings")
