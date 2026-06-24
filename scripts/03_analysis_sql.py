import csv
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_TABLE_DIR = PROJECT_ROOT / "outputs" / "tables"

HDFS_BASE = "hdfs://localhost:9000/user/muxin/amazon_video_games"
REVIEWS_CLEAN_PATH = f"{HDFS_BASE}/clean/reviews_clean"
REVIEWS5_CLEAN_PATH = f"{HDFS_BASE}/clean/reviews5_clean"
META_CLEAN_PATH = f"{HDFS_BASE}/clean/meta_clean"
HDFS_TABLE_DIR = f"{HDFS_BASE}/outputs/tables"

HIGH_FREQ_REVIEW_THRESHOLD = 10

STOPWORDS = sorted({
    "the", "and", "for", "with", "this", "that", "these", "those", "you",
    "your", "are", "was", "were", "have", "has", "had", "but", "not", "all",
    "can", "don", "didn", "doesn", "do", "does", "did", "get", "got", "out",
    "one", "two", "just", "from", "they", "them", "his", "her", "she", "him",
    "its", "our", "their", "there", "then", "than", "too", "very", "more",
    "most", "some", "any", "other", "same", "into", "about", "would",
    "could", "should", "when", "what", "which", "who", "why", "how", "been",
    "will", "also", "only", "really", "much", "many", "because", "after",
    "before", "over", "under", "again", "first", "now", "way", "back", "off",
    "thing", "lot", "little", "game", "games", "play", "played", "playing",
    "product", "amazon", "video", "review", "buy", "bought", "use", "used",
    "using", "like", "make", "made", "time", "even", "still", "well", "good"
})


def build_spark():
    return (
        SparkSession.builder
        .appName("Amazon Video Games SQL Analysis")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "32m")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def save_result(df, name):
    """Save a small aggregated Spark result as local CSV and HDFS CSV directory."""
    LOCAL_TABLE_DIR.mkdir(parents=True, exist_ok=True)

    cached_df = df.cache()
    local_path = LOCAL_TABLE_DIR / f"{name}.csv"
    hdfs_path = f"{HDFS_TABLE_DIR}/{name}"

    try:
        rows = cached_df.collect()
        with local_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(cached_df.columns)
            for row in rows:
                writer.writerow([row[column] for column in cached_df.columns])

        (
            cached_df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(hdfs_path)
        )
    finally:
        try:
            cached_df.unpersist()
        except Exception:
            pass

    print(f"Saved local CSV: {local_path}")
    print(f"Saved HDFS CSV directory: {hdfs_path}")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "24")

    reviews = spark.read.parquet(REVIEWS_CLEAN_PATH)
    reviews5 = spark.read.parquet(REVIEWS5_CLEAN_PATH)
    meta = spark.read.parquet(META_CLEAN_PATH)

    reviews.createOrReplaceTempView("reviews_clean")
    reviews5.createOrReplaceTempView("reviews5_clean")
    meta.createOrReplaceTempView("meta_clean")

    analyses = {
        "01_yearly_review_trend": """
            SELECT
                review_year,
                COUNT(*) AS review_count,
                ROUND(AVG(rating), 3) AS avg_rating,
                SUM(CASE WHEN verified THEN 1 ELSE 0 END) AS verified_review_count
            FROM reviews_clean
            WHERE review_year IS NOT NULL
            GROUP BY review_year
            ORDER BY review_year
        """,

        "02_rating_distribution": """
            SELECT
                rating,
                COUNT(*) AS review_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS review_pct
            FROM reviews_clean
            WHERE rating IS NOT NULL
            GROUP BY rating
            ORDER BY rating
        """,

        "03_verified_rating_difference": """
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
            ORDER BY CASE WHEN verified THEN 1 ELSE 2 END
        """,

        "04_helpful_votes_relationship": """
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
            ORDER BY rating, review_length_bucket_order
        """,

        "05_user_frequency_behavior_reviews5": f"""
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
                        WHEN u.user_review_count >= {HIGH_FREQ_REVIEW_THRESHOLD}
                            THEN 'high_frequency_user'
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
            ORDER BY CASE WHEN user_group = 'high_frequency_user' THEN 1 ELSE 2 END
        """,

        "06_price_bucket_rating_reviews": """
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
            ORDER BY price_bucket_order
        """,

        "07_brand_review_rating_ranking": """
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
            LIMIT 30
        """,

        "08_high_low_rating_keywords": f"""
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
                  AND token NOT IN ({", ".join("'" + word + "'" for word in STOPWORDS)})
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
            ORDER BY sentiment_group, keyword_rank
        """
    }

    for name, query in analyses.items():
        print(f"Running analysis: {name}")
        result = spark.sql(query)
        save_result(result, name)

    print("All SQL analysis results saved.")
    spark.stop()


if __name__ == "__main__":
    main()
