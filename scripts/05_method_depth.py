import csv
import sys
import types
from functools import total_ordering
from pathlib import Path


def ensure_distutils_compat():
    """Provide distutils.version.LooseVersion for PySpark on Python 3.13."""
    try:
        import distutils.version  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    from packaging.version import parse as parse_version

    @total_ordering
    class LooseVersion:
        def __init__(self, version):
            self.vstring = str(version)
            self._parsed = parse_version(self.vstring)
            self.version = self.vstring.replace("-", ".").split(".")

        def _coerce(self, other):
            if isinstance(other, LooseVersion):
                return other._parsed
            return parse_version(str(other))

        def __lt__(self, other):
            return self._parsed < self._coerce(other)

        def __eq__(self, other):
            return self._parsed == self._coerce(other)

        def __repr__(self):
            return f"LooseVersion('{self.vstring}')"

    distutils_module = types.ModuleType("distutils")
    version_module = types.ModuleType("distutils.version")
    version_module.LooseVersion = LooseVersion
    distutils_module.version = version_module
    sys.modules["distutils"] = distutils_module
    sys.modules["distutils.version"] = version_module


ensure_distutils_compat()

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, when


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_TABLE_DIR = PROJECT_ROOT / "outputs" / "tables"

HDFS_BASE = "hdfs://localhost:9000/user/muxin/amazon_video_games"
REVIEWS_CLEAN_PATH = f"{HDFS_BASE}/clean/reviews_clean"
META_CLEAN_PATH = f"{HDFS_BASE}/clean/meta_clean"
HDFS_TABLE_DIR = f"{HDFS_BASE}/outputs/tables"

FEATURE_COLUMNS = [
    "rating",
    "verified_numeric",
    "review_length",
    "review_year",
    "price",
]


def build_spark():
    return (
        SparkSession.builder
        .appName("Amazon Video Games Helpful Review Prediction")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "32m")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def save_small_result(df, name):
    """Save small Spark DataFrames to local CSV and HDFS CSV directory."""
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


def get_price_fill_value(meta):
    """Use approximate median metadata price, falling back to 0 if unavailable."""
    row = (
        meta
        .where(col("price").isNotNull())
        .selectExpr("percentile_approx(price, 0.5, 10000) AS median_price")
        .first()
    )

    median_price = row["median_price"] if row is not None else None
    if median_price is None:
        return 0.0, "fallback_zero_price"
    return float(median_price), "median_meta_price"


def prepare_model_data(reviews, meta, price_fill_value):
    reviews_features = reviews.select(
        "asin",
        col("rating").cast("double").alias("rating"),
        when(col("verified") == True, lit(1.0)).otherwise(lit(0.0)).alias("verified_numeric"),
        col("review_length").cast("double").alias("review_length"),
        col("review_year").cast("double").alias("review_year"),
        when(col("helpful_votes") > 0, lit(1.0)).otherwise(lit(0.0)).alias("is_helpful"),
    )

    meta_features = meta.select(
        "asin",
        col("price").cast("double").alias("meta_price"),
    ).dropDuplicates(["asin"])

    joined = (
        reviews_features
        .join(meta_features, on="asin", how="left")
        .withColumn("price", when(col("meta_price").isNull(), lit(price_fill_value)).otherwise(col("meta_price")))
        .drop("meta_price")
    )

    # LogisticRegression cannot handle null numeric feature values.
    return joined.where(
        col("rating").isNotNull()
        & col("review_length").isNotNull()
        & col("review_year").isNotNull()
    )


def add_features(df):
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="skip",
    )

    return assembler.transform(df).select("features", "is_helpful")


def build_metrics_df(spark, model, predictions, train_count, test_count, price_fill_value, price_fill_strategy):
    evaluator_roc = BinaryClassificationEvaluator(
        labelCol="is_helpful",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="is_helpful",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )

    area_under_roc = evaluator_roc.evaluate(predictions)
    area_under_pr = evaluator_pr.evaluate(predictions)

    prediction_summary = (
        predictions
        .select("is_helpful", "prediction")
        .withColumn("correct", when(col("is_helpful") == col("prediction"), lit(1.0)).otherwise(lit(0.0)))
        .agg(
            count("*").alias("evaluated_rows"),
            avg("correct").alias("accuracy"),
            avg("is_helpful").alias("positive_label_rate"),
            avg("prediction").alias("positive_prediction_rate"),
        )
        .first()
    )

    metrics = [
        ("model", "logistic_regression"),
        ("label_definition", "is_helpful = 1 if helpful_votes > 0 else 0"),
        ("price_fill_strategy", price_fill_strategy),
        ("price_fill_value", f"{price_fill_value:.6f}"),
        ("train_rows", str(train_count)),
        ("test_rows", str(test_count)),
        ("evaluated_rows", str(prediction_summary["evaluated_rows"])),
        ("area_under_roc", f"{area_under_roc:.6f}"),
        ("area_under_pr", f"{area_under_pr:.6f}"),
        ("accuracy", f"{prediction_summary['accuracy']:.6f}"),
        ("test_positive_label_rate", f"{prediction_summary['positive_label_rate']:.6f}"),
        ("test_positive_prediction_rate", f"{prediction_summary['positive_prediction_rate']:.6f}"),
        ("max_iter", str(model.getMaxIter())),
        ("reg_param", f"{model.getRegParam():.6f}"),
        ("elastic_net_param", f"{model.getElasticNetParam():.6f}"),
    ]

    return spark.createDataFrame(metrics, ["metric", "value"])


def build_coefficients_df(spark, model):
    coefficients = [
        (feature, float(coefficient), abs(float(coefficient)))
        for feature, coefficient in zip(FEATURE_COLUMNS, model.coefficients)
    ]
    coefficients.append(("intercept", float(model.intercept), abs(float(model.intercept))))

    return spark.createDataFrame(
        coefficients,
        ["feature", "coefficient", "abs_coefficient"],
    ).orderBy(col("abs_coefficient").desc())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "24")

    reviews = spark.read.parquet(REVIEWS_CLEAN_PATH)
    meta = spark.read.parquet(META_CLEAN_PATH)

    price_fill_value, price_fill_strategy = get_price_fill_value(meta)
    print(f"Price fill strategy: {price_fill_strategy}; value: {price_fill_value:.6f}")

    model_data = prepare_model_data(reviews, meta, price_fill_value)
    featured_data = add_features(model_data).cache()

    train_data, test_data = featured_data.randomSplit([0.8, 0.2], seed=42)
    train_data = train_data.cache()
    test_data = test_data.cache()

    train_count = train_data.count()
    test_count = test_data.count()

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="is_helpful",
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0,
    )

    model = lr.fit(train_data)
    predictions = model.transform(test_data).cache()

    metrics_df = build_metrics_df(
        spark,
        model,
        predictions,
        train_count,
        test_count,
        price_fill_value,
        price_fill_strategy,
    )
    coefficients_df = build_coefficients_df(spark, model)

    save_small_result(metrics_df, "09_helpful_prediction_metrics")
    save_small_result(coefficients_df, "09_helpful_prediction_coefficients")

    predictions.unpersist()
    train_data.unpersist()
    test_data.unpersist()
    featured_data.unpersist()

    print("Helpful review prediction outputs saved.")
    spark.stop()


if __name__ == "__main__":
    main()
