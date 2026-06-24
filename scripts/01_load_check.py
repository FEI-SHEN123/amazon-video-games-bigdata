from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Amazon Video Games Load Check") \
    .getOrCreate()

review_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games.json.gz"
review5_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games_5.json.gz"
meta_path = "hdfs://localhost:9000/user/muxin/amazon_video_games/raw/meta_Video_Games.json.gz"

reviews = spark.read.json(review_path)
reviews5 = spark.read.json(review5_path)
meta = spark.read.json(meta_path)

print("Full reviews count:", reviews.count())
print("5-core reviews count:", reviews5.count())
print("Metadata count:", meta.count())

print("Full reviews schema:")
reviews.printSchema()

print("Metadata schema:")
meta.printSchema()

print("Reviews sample:")
reviews.select("reviewerID", "asin", "overall", "verified", "reviewTime", "summary").show(5, truncate=False)

print("Metadata sample:")
meta.select("asin", "title", "brand", "price").show(5, truncate=False)

spark.stop()
