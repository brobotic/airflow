-- 1. Row count and rated %
SELECT
    COUNT(*)                                              AS total_titles,
    COUNT(average_rating)                                 AS rated_titles,
    ROUND(COUNT(average_rating)::numeric / COUNT(*) * 100, 1) AS pct_rated
FROM mart_titles_enriched;

-- 2. Rating bucket distribution
SELECT rating_bucket, COUNT(*) AS cnt
FROM mart_titles_enriched
GROUP BY rating_bucket
ORDER BY cnt DESC;

-- 3. Top 10 movies by votes
SELECT primary_title, start_year, average_rating, num_votes
FROM mart_titles_enriched
WHERE title_type = 'movie'
ORDER BY num_votes DESC NULLS LAST
LIMIT 10;

-- 4. Title count per era
SELECT era, COUNT(*) AS cnt
FROM mart_titles_enriched
GROUP BY era
ORDER BY era NULLS LAST;

-- 5. Average rating by title type
SELECT
    title_type,
    ROUND(AVG(average_rating)::numeric, 2) AS avg_rating,
    COUNT(*) AS total
FROM mart_titles_enriched
WHERE average_rating IS NOT NULL
GROUP BY title_type
ORDER BY avg_rating DESC;