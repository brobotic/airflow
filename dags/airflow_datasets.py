from airflow.datasets import Dataset


TITLE_BASICS_DATASET = Dataset("imdb://postgres_movies/public/title_basics")
NAME_BASICS_DATASET = Dataset("imdb://postgres_movies/public/name_basics")
TITLE_CREW_DATASET = Dataset("imdb://postgres_movies/public/title_crew")
TITLE_PRINCIPALS_DATASET = Dataset("imdb://postgres_movies/public/title_principals")
TITLE_RATINGS_DATASET = Dataset("imdb://postgres_movies/public/title_ratings")
TITLE_AKAS_DATASET = Dataset("imdb://postgres_movies/public/title_akas")
TITLE_EPISODE_DATASET = Dataset("imdb://postgres_movies/public/title_episode")
MART_EPISODE_CREDITS_DATASET = Dataset("imdb://postgres_movies/public/mart_episode_credits")
MART_EPISODE_ENRICHED_DATASET = Dataset("imdb://postgres_movies/public/mart_episode_enriched")
LETTERBOXD_DIARY_DATASET = Dataset("letterboxd://postgres_movies/public/letterboxd_diary")
MART_LETTERBOXD_DIARY_DATASET = Dataset("letterboxd://postgres_movies/public/mart_letterboxd_diary")
MART_LETTERBOXD_MOVIE_MATCHES_DATASET = Dataset("letterboxd://postgres_movies/public/mart_letterboxd_movie_matches")
