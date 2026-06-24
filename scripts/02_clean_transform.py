from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, lit, length, regexp_replace, regexp_extract,
    from_unixtime, to_date, year, month, when
)

spark = SparkSession.builder \
    .appName("Amazon Video Games Clean Transform") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

review_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games.json.gz"
review5_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games_5.json.gz"
meta_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/meta_Video_Games.json.gz"

clean_review_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/clean/reviews_clean"
clean_review5_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/clean/reviews5_clean"
clean_meta_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/clean/meta_clean"

reviews = spark.read.json(review_path)
reviews5 = spark.read.json(review5_path)
meta = spark.read.json(meta_path)

def clean_reviews(df):
    return df.select(
        col("reviewerID"),
        col("asin"),
        col("overall").cast("double").alias("rating"),
        col("verified").cast("boolean").alias("verified"),
        col("reviewText"),
        col("summary"),
        col("unixReviewTime").cast("long").alias("unix_time"),
        regexp_replace(coalesce(col("vote").cast("string"), lit("0")), ",", "").cast("int").alias("helpful_votes")
    ).filter(
        col("reviewerID").isNotNull() &
        col("asin").isNotNull() &
        col("rating").isNotNull()
    ).withColumn(
        "review_date", to_date(from_unixtime(col("unix_time")))
    ).withColumn(
        "review_year", year(col("review_date"))
    ).withColumn(
        "review_month", month(col("review_date"))
    ).withColumn(
        "review_length", length(coalesce(col("reviewText"), lit("")))
    ).withColumn(
        "has_review_text", when(length(coalesce(col("reviewText"), lit(""))) > 0, lit(1)).otherwise(lit(0))
    ).dropDuplicates(["reviewerID", "asin", "unix_time"])

reviews_clean = clean_reviews(reviews)
reviews5_clean = clean_reviews(reviews5)

meta_clean = meta.select(
    col("asin"),
    col("title"),
    regexp_replace(coalesce(col("brand"), lit("")), r"by\s+", "").alias("brand_raw"),
    col("price").cast("string").alias("price_raw"),
    col("also_buy"),
    col("also_view")
).filter(
    col("asin").isNotNull()
).withColumn(
    "brand",
    regexp_replace(regexp_replace(col("brand_raw"), "\n", " "), r"\s+", " ")
).withColumn(
    "price",
    regexp_extract(col("price_raw"), r"\$([0-9]+(?:\.[0-9]+)?)", 1).cast("double")
).drop("brand_raw").dropDuplicates(["asin"])

print("Raw full reviews:", reviews.count())
print("Clean full reviews:", reviews_clean.count())
print("Raw 5-core reviews:", reviews5.count())
print("Clean 5-core reviews:", reviews5_clean.count())
print("Raw metadata:", meta.count())
print("Clean metadata:", meta_clean.count())

reviews_clean.write.mode("overwrite").parquet(clean_review_path)
reviews5_clean.write.mode("overwrite").parquet(clean_review5_path)
meta_clean.write.mode("overwrite").parquet(clean_meta_path)

print("Cleaned data saved to HDFS parquet.")
spark.stop()
