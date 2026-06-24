-- Amazon Video Games Review Analysis
-- Spark SQL query reference extracted from scripts/03_analysis_sql.py.
--
-- Purpose:
--   This file is a readable and reproducible experiment note for the eight
--   core analysis questions. It uses the temporary table names registered by
--   scripts/03_analysis_sql.py:
--     reviews_clean
--     reviews5_clean
--     meta_clean
--
-- Notes:
--   - This is Spark SQL style documentation, not a Hive execution script.
--   - Query 05 defines high-frequency users as reviewers with at least
--     10 reviews in reviews5_clean.
--   - Query 08 keeps meaningful game-review words such as graphics,
--     controller, mouse, fun, work, money, waste, quality, disappointed,
--     broken, and recommend by filtering only generic stopwords, short tokens,
--     and purely numeric tokens.


-- Question 01: yearly review trend
-- Goal: Count reviews by year and track average rating and verified review volume.
SELECT
    review_year,
    COUNT(*) AS review_count,
    ROUND(AVG(rating), 3) AS avg_rating,
    SUM(CASE WHEN verified THEN 1 ELSE 0 END) AS verified_review_count
FROM reviews_clean
WHERE review_year IS NOT NULL
GROUP BY review_year
ORDER BY review_year;


-- Question 02: rating distribution
-- Goal: Show how reviews are distributed across rating scores.
SELECT
    rating,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS review_pct
FROM reviews_clean
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY rating;


-- Question 03: verified purchase rating difference
-- Goal: Compare rating, helpful votes, and review length between verified and
-- non-verified purchases.
SELECT
    CASE
        WHEN verified THEN 'verified_purchase'
        ELSE 'non_verified_purchase'
    END AS purchase_type,
    COUNT(*) AS review_count,
    ROUND(AVG(rating), 3) AS avg_rating,
    ROUND(STDDEV(rating), 3) AS rating_stddev,
    ROUND(AVG(helpful_votes), 3) AS avg_helpful_votes,
    ROUND(AVG(review_length), 1) AS avg_review_length
FROM reviews_clean
WHERE verified IS NOT NULL
GROUP BY verified
ORDER BY CASE WHEN verified THEN 1 ELSE 2 END;


-- Question 04: helpful votes relationship
-- Goal: Analyze helpful votes by rating and review length bucket.
WITH bucketed AS (
    SELECT
        CAST(rating AS INT) AS rating,
        helpful_votes,
        review_length,
        CASE
            WHEN review_length < 100 THEN 1
            WHEN review_length < 300 THEN 2
            WHEN review_length < 700 THEN 3
            WHEN review_length < 1500 THEN 4
            ELSE 5
        END AS review_length_bucket_order,
        CASE
            WHEN review_length < 100 THEN '0-99 chars'
            WHEN review_length < 300 THEN '100-299 chars'
            WHEN review_length < 700 THEN '300-699 chars'
            WHEN review_length < 1500 THEN '700-1499 chars'
            ELSE '1500+ chars'
        END AS review_length_bucket
    FROM reviews_clean
    WHERE rating IS NOT NULL
      AND helpful_votes IS NOT NULL
      AND review_length IS NOT NULL
)
SELECT
    rating,
    review_length_bucket_order,
    review_length_bucket,
    COUNT(*) AS review_count,
    ROUND(AVG(helpful_votes), 3) AS avg_helpful_votes,
    PERCENTILE_APPROX(helpful_votes, 0.5) AS median_helpful_votes,
    ROUND(AVG(review_length), 1) AS avg_review_length
FROM bucketed
GROUP BY rating, review_length_bucket_order, review_length_bucket
ORDER BY rating, review_length_bucket_order;


-- Question 05: high-frequency user behavior
-- Goal: Compare rating behavior for high-frequency users and ordinary users
-- using the 5-core review dataset.
WITH user_stats AS (
    SELECT
        reviewerID,
        COUNT(*) AS user_review_count
    FROM reviews5_clean
    GROUP BY reviewerID
),
labeled_reviews AS (
    SELECT
        r.*,
        u.user_review_count,
        CASE
            WHEN u.user_review_count >= 10 THEN 'high_frequency_user'
            ELSE 'ordinary_user'
        END AS user_group
    FROM reviews5_clean r
    INNER JOIN user_stats u
        ON r.reviewerID = u.reviewerID
)
SELECT
    user_group,
    COUNT(DISTINCT reviewerID) AS user_count,
    COUNT(*) AS review_count,
    ROUND(AVG(user_review_count), 2) AS avg_reviews_per_user,
    ROUND(AVG(rating), 3) AS avg_rating,
    ROUND(STDDEV(rating), 3) AS rating_stddev,
    ROUND(AVG(helpful_votes), 3) AS avg_helpful_votes,
    ROUND(AVG(review_length), 1) AS avg_review_length,
    ROUND(SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS positive_review_pct
FROM labeled_reviews
GROUP BY user_group
ORDER BY CASE WHEN user_group = 'high_frequency_user' THEN 1 ELSE 2 END;


-- Question 06: price bucket rating and review count
-- Goal: Join reviews with metadata to compare rating and review volume across
-- product price buckets.
WITH joined_reviews AS (
    SELECT
        r.asin,
        r.rating,
        m.price
    FROM reviews_clean r
    INNER JOIN meta_clean m
        ON r.asin = m.asin
    WHERE m.price IS NOT NULL
      AND m.price >= 0
      AND r.rating IS NOT NULL
),
bucketed AS (
    SELECT
        asin,
        rating,
        price,
        CASE
            WHEN price < 10 THEN 1
            WHEN price < 25 THEN 2
            WHEN price < 50 THEN 3
            WHEN price < 75 THEN 4
            WHEN price < 100 THEN 5
            ELSE 6
        END AS price_bucket_order,
        CASE
            WHEN price < 10 THEN '$0-9.99'
            WHEN price < 25 THEN '$10-24.99'
            WHEN price < 50 THEN '$25-49.99'
            WHEN price < 75 THEN '$50-74.99'
            WHEN price < 100 THEN '$75-99.99'
            ELSE '$100+'
        END AS price_bucket
    FROM joined_reviews
)
SELECT
    price_bucket_order,
    price_bucket,
    COUNT(*) AS review_count,
    COUNT(DISTINCT asin) AS product_count,
    ROUND(AVG(rating), 3) AS avg_rating,
    ROUND(STDDEV(rating), 3) AS rating_stddev,
    ROUND(AVG(price), 2) AS avg_price
FROM bucketed
GROUP BY price_bucket_order, price_bucket
ORDER BY price_bucket_order;


-- Question 07: brand review/rating ranking
-- Goal: Rank brands by review count and average rating after joining reviews
-- with product metadata.
WITH joined_reviews AS (
    SELECT
        COALESCE(NULLIF(TRIM(m.brand), ''), 'Unknown') AS brand,
        r.asin,
        r.rating
    FROM reviews_clean r
    INNER JOIN meta_clean m
        ON r.asin = m.asin
    WHERE r.rating IS NOT NULL
)
SELECT
    brand,
    COUNT(*) AS review_count,
    COUNT(DISTINCT asin) AS product_count,
    ROUND(AVG(rating), 3) AS avg_rating,
    ROUND(STDDEV(rating), 3) AS rating_stddev
FROM joined_reviews
GROUP BY brand
HAVING COUNT(*) >= 20
ORDER BY review_count DESC, avg_rating DESC
LIMIT 30;


-- Question 08: high/low rating keywords
-- Goal: Compare frequent keywords in high-rating reviews and low-rating reviews.
WITH text_reviews AS (
    SELECT
        CASE
            WHEN rating >= 4 THEN 'high_rating'
            WHEN rating <= 2 THEN 'low_rating'
        END AS sentiment_group,
        LOWER(
            REGEXP_REPLACE(
                CONCAT_WS(
                    ' ',
                    COALESCE(summary, ''),
                    SUBSTR(COALESCE(reviewText, ''), 1, 2000)
                ),
                '[^a-zA-Z0-9 ]',
                ' '
            )
        ) AS review_text
    FROM reviews_clean
    WHERE rating >= 4 OR rating <= 2
),
tokens AS (
    SELECT
        sentiment_group,
        token
    FROM text_reviews
    LATERAL VIEW EXPLODE(SPLIT(review_text, ' +')) exploded AS token
),
clean_tokens AS (
    SELECT
        sentiment_group,
        token
    FROM tokens
    WHERE LENGTH(token) >= 3
      AND token NOT RLIKE '^[0-9]+$'
      AND token NOT IN (
          'about', 'after', 'again', 'all', 'also', 'amazon', 'and', 'any',
          'are', 'back', 'because', 'been', 'before', 'bought', 'but', 'buy',
          'can', 'could', 'did', 'didn', 'do', 'does', 'doesn', 'don',
          'even', 'first', 'for', 'from', 'game', 'games', 'get', 'good',
          'got', 'had', 'has', 'have', 'her', 'him', 'his', 'how',
          'into', 'its', 'just', 'like', 'little', 'lot', 'made', 'make',
          'many', 'more', 'most', 'much', 'not', 'now', 'off', 'one',
          'only', 'other', 'our', 'out', 'over', 'play', 'played', 'playing',
          'product', 'really', 'review', 'same', 'she', 'should', 'some', 'still',
          'than', 'that', 'the', 'their', 'them', 'then', 'there', 'these',
          'they', 'thing', 'this', 'those', 'time', 'too', 'two', 'under',
          'use', 'used', 'using', 'very', 'video', 'was', 'way', 'well',
          'were', 'what', 'when', 'which', 'who', 'why', 'will', 'with',
          'would', 'you', 'your'
      )
),
keyword_counts AS (
    SELECT
        sentiment_group,
        token AS keyword,
        COUNT(*) AS keyword_count
    FROM clean_tokens
    GROUP BY sentiment_group, token
),
keyword_totals AS (
    SELECT
        sentiment_group,
        SUM(keyword_count) AS total_keywords
    FROM keyword_counts
    GROUP BY sentiment_group
),
ranked AS (
    SELECT
        c.sentiment_group,
        c.keyword,
        c.keyword_count,
        ROUND(c.keyword_count * 10000.0 / t.total_keywords, 2) AS frequency_per_10k_words,
        ROW_NUMBER() OVER (
            PARTITION BY c.sentiment_group
            ORDER BY c.keyword_count DESC
        ) AS keyword_rank
    FROM keyword_counts c
    INNER JOIN keyword_totals t
        ON c.sentiment_group = t.sentiment_group
)
SELECT
    sentiment_group,
    keyword_rank,
    keyword,
    keyword_count,
    frequency_per_10k_words
FROM ranked
WHERE keyword_rank <= 40
ORDER BY sentiment_group, keyword_rank;
