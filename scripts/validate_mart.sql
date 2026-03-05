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
    COUNT(*) FILTER (WHERE dop_nconsts IS NOT NULL AND dop_nconsts <> '') AS with_dop,
    COUNT(*) FILTER (WHERE average_rating IS NOT NULL) AS with_rating
FROM mart_movie_credits;

-- 8. movie_credits: top 10 movies by votes
SELECT
    primary_title,
    start_year,
    directors_names,
    dop_names,
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