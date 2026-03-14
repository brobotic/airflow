-- 1. Mart row counts + freshness
SELECT
    mart_name,
    row_count,
    last_refreshed_at
FROM (
    SELECT
        'mart_titles_enriched' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_titles_enriched

    UNION ALL

    SELECT
        'mart_movie_credits' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_movie_credits

    UNION ALL

    SELECT
        'mart_director_credits' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_director_credits

    UNION ALL

    SELECT
        'mart_episode_credits' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_episode_credits

    UNION ALL

    SELECT
        'mart_series_people_rollup' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_series_people_rollup

    UNION ALL

    SELECT
        'mart_episode_enriched' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_episode_enriched

    UNION ALL

    SELECT
        'mart_series_akas' AS mart_name,
        COUNT(*)::bigint AS row_count,
        MAX(last_refreshed_at) AS last_refreshed_at
    FROM mart_series_akas
) s
ORDER BY mart_name;

-- 2. titles_enriched: row count and rated %
SELECT
    COUNT(*) AS total_titles,
    COUNT(average_rating) AS rated_titles,
    ROUND(COUNT(average_rating)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS pct_rated
FROM mart_titles_enriched;

-- 3. titles_enriched: rating bucket distribution
SELECT rating_bucket, COUNT(*) AS cnt
FROM mart_titles_enriched
GROUP BY rating_bucket
ORDER BY cnt DESC;

-- 4. titles_enriched: top 10 movies by votes
SELECT primary_title, start_year, average_rating, num_votes
FROM mart_titles_enriched
WHERE title_type = 'movie'
ORDER BY num_votes DESC NULLS LAST
LIMIT 10;

-- 5. titles_enriched: title count per era
SELECT era, COUNT(*) AS cnt
FROM mart_titles_enriched
GROUP BY era
ORDER BY era NULLS LAST;

-- 6. titles_enriched: average rating by title type
SELECT
    title_type,
    ROUND(AVG(average_rating)::numeric, 2) AS avg_rating,
    COUNT(*) AS total
FROM mart_titles_enriched
WHERE average_rating IS NOT NULL
GROUP BY title_type
ORDER BY avg_rating DESC;

-- 7. movie_credits: coverage summary
SELECT
    COUNT(*) AS total_movies,
    COUNT(*) FILTER (WHERE directors_nconsts IS NOT NULL AND directors_nconsts <> '') AS with_directors,
    COUNT(*) FILTER (WHERE actors_nconsts IS NOT NULL AND actors_nconsts <> '') AS with_actors,
    COUNT(*) FILTER (WHERE dop_nconsts IS NOT NULL AND dop_nconsts <> '') AS with_dop,
    COUNT(*) FILTER (WHERE editor_nconsts IS NOT NULL AND editor_nconsts <> '') AS with_editor,
    COUNT(*) FILTER (WHERE composer_nconsts IS NOT NULL AND composer_nconsts <> '') AS with_composer,
    COUNT(*) FILTER (WHERE average_rating IS NOT NULL) AS with_rating
FROM mart_movie_credits;

-- 8. movie_credits: top 10 movies by votes
SELECT
    primary_title,
    start_year,
    directors_names,
    actors_names,
    dop_names,
    editor_names,
    composer_names,
    average_rating,
    num_votes
FROM mart_movie_credits
ORDER BY num_votes DESC NULLS LAST, average_rating DESC NULLS LAST
LIMIT 10;

-- 9. movie_credits: movies per year (top 15 years)
SELECT
    start_year,
    COUNT(*) AS movie_count
FROM mart_movie_credits
GROUP BY start_year
ORDER BY movie_count DESC, start_year DESC NULLS LAST
LIMIT 15;

-- 10. director_credits: coverage summary
SELECT
    COUNT(*) AS total_director_rows,
    COUNT(*) FILTER (WHERE dop_nconst IS NOT NULL) AS with_dop,
    COUNT(*) FILTER (WHERE avg_rating IS NOT NULL) AS with_avg_rating,
    SUM(movie_count)::bigint AS summed_movie_count,
    SUM(total_votes)::bigint AS summed_votes
FROM mart_director_credits;

-- 11. director_credits: top 10 director + DoP relationships by movie_count
SELECT
    director_name,
    dop_name,
    movie_count,
    avg_rating,
    total_votes,
    first_movie_year,
    last_movie_year
FROM mart_director_credits
ORDER BY movie_count DESC, total_votes DESC
LIMIT 10;

-- 12. director_credits: top 10 directors by total votes (aggregate over DoP rows)
SELECT
    director_nconst,
    MAX(director_name) AS director_name,
    COUNT(*) AS relationship_rows,
    SUM(movie_count)::bigint AS total_movie_count,
    SUM(total_votes)::bigint AS total_votes,
    ROUND(AVG(avg_rating)::numeric, 2) AS avg_relationship_rating
FROM mart_director_credits
GROUP BY director_nconst
ORDER BY total_votes DESC NULLS LAST
LIMIT 10;

-- 13. episode_credits: coverage summary
SELECT
    COUNT(*) AS total_credit_rows,
    COUNT(DISTINCT series_tconst) AS series_count,
    COUNT(DISTINCT episode_tconst) AS episode_count,
    COUNT(DISTINCT person_nconst) AS people_count,
    COUNT(*) FILTER (WHERE average_rating IS NOT NULL) AS rows_with_rating
FROM mart_episode_credits;

-- 14. episode_credits: credit type distribution
SELECT
    credit_type,
    COUNT(*) AS cnt
FROM mart_episode_credits
GROUP BY credit_type
ORDER BY cnt DESC, credit_type;

-- 15. episode_credits: top contributors by episode coverage
SELECT
    person_name,
    credit_type,
    COUNT(DISTINCT episode_tconst) AS episode_count,
    COUNT(DISTINCT series_tconst) AS series_count
FROM mart_episode_credits
GROUP BY person_name, credit_type
ORDER BY episode_count DESC, series_count DESC
LIMIT 15;

-- 16. series_people_rollup: quality and coverage summary
SELECT
    COUNT(*) AS total_rollup_rows,
    COUNT(DISTINCT series_tconst) AS series_count,
    COUNT(DISTINCT person_nconst) AS people_count,
    ROUND(AVG(episode_count)::numeric, 2) AS avg_episode_coverage,
    ROUND(AVG(avg_episode_rating)::numeric, 2) AS avg_episode_rating
FROM mart_series_people_rollup;

-- 17. series_people_rollup: top people by episode coverage
SELECT
    series_title,
    person_name,
    credit_type,
    episode_count,
    season_count,
    avg_episode_rating,
    total_episode_votes
FROM mart_series_people_rollup
ORDER BY episode_count DESC, total_episode_votes DESC NULLS LAST
LIMIT 15;

-- 18. episode_enriched: coverage summary
SELECT
    COUNT(*) AS total_episodes,
    COUNT(*) FILTER (WHERE average_rating IS NOT NULL) AS episodes_with_rating,
    ROUND(AVG(average_rating)::numeric, 2) AS avg_rating,
    SUM(episode_aka_count)::bigint AS total_episode_aka_rows
FROM mart_episode_enriched;

-- 19. episode_enriched: top episodes by votes
SELECT
    series_title,
    season_number,
    episode_number,
    episode_title,
    average_rating,
    num_votes,
    episode_aka_count
FROM mart_episode_enriched
ORDER BY num_votes DESC NULLS LAST, average_rating DESC NULLS LAST
LIMIT 15;

-- 20. series_akas: localization coverage summary
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT episode_tconst) AS localized_episodes,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT language) AS language_count,
    SUM(aka_count)::bigint AS total_aka_count
FROM mart_series_akas;

-- 21. series_akas: top region/language combinations by AKA volume
SELECT
    region,
    language,
    SUM(aka_count)::bigint AS aka_count
FROM mart_series_akas
GROUP BY region, language
ORDER BY aka_count DESC, region, language
LIMIT 15;

-- 22. LOST analytics: episode and contributor coverage
SELECT
    COUNT(DISTINCT episode_tconst) AS lost_episode_count,
    COUNT(DISTINCT person_nconst) AS lost_people_count,
    COUNT(*) AS lost_credit_rows
FROM mart_episode_credits
WHERE lower(series_title) = 'lost';

-- 23. LOST analytics: top contributors by episode_count
SELECT
    person_name,
    credit_type,
    episode_count,
    season_count,
    avg_episode_rating,
    total_episode_votes
FROM mart_series_people_rollup
WHERE lower(series_title) = 'lost'
ORDER BY episode_count DESC, total_episode_votes DESC NULLS LAST
LIMIT 20;

-- 24. LOST analytics: top episodes by votes and rating
SELECT
    season_number,
    episode_number,
    episode_title,
    average_rating,
    num_votes,
    episode_aka_count,
    episode_regions,
    episode_languages
FROM mart_episode_enriched
WHERE lower(series_title) = 'lost'
ORDER BY num_votes DESC NULLS LAST, average_rating DESC NULLS LAST
LIMIT 20;

-- 25. letterboxd_diary mart: row count + freshness by metric_type
SELECT
    metric_type,
    COUNT(*) AS row_count,
    MAX(last_refreshed_at) AS last_refreshed_at
FROM mart_letterboxd_diary
GROUP BY metric_type
ORDER BY metric_type;

-- 26. letterboxd_diary source: most-logged movies
SELECT
    film_name,
    film_year,
    COUNT(*) AS diary_entries,
    ROUND(AVG(rating)::numeric, 2) AS avg_rating,
    COUNT(*) FILTER (WHERE COALESCE(rewatch, FALSE)) AS rewatch_count,
    MIN(COALESCE(watched_date, entry_date)) AS first_watch_date,
    MAX(COALESCE(watched_date, entry_date)) AS last_watch_date
FROM letterboxd_diary
GROUP BY film_name, film_year
ORDER BY diary_entries DESC, film_name
LIMIT 15;

-- 27. letterboxd_diary source: all entries for top logged movie
WITH top_movie AS (
    SELECT
        film_name,
        film_year
    FROM letterboxd_diary
    GROUP BY film_name, film_year
    ORDER BY COUNT(*) DESC, film_name
    LIMIT 1
)
SELECT
    COALESCE(d.watched_date, d.entry_date) AS activity_date,
    d.entry_date,
    d.watched_date,
    d.film_name,
    d.film_year,
    d.rating,
    d.rewatch,
    d.tags,
    d.letterboxd_uri,
    d.diary_id
FROM letterboxd_diary d
JOIN top_movie m
    ON d.film_name = m.film_name
   AND d.film_year IS NOT DISTINCT FROM m.film_year
ORDER BY COALESCE(d.watched_date, d.entry_date) DESC NULLS LAST, d.diary_id DESC;