# Amazon Video Games Review Analysis

This Big Data final project analyzes Amazon Video Games review data with PySpark,
Spark SQL, pandas, matplotlib, and seaborn. The project cleans raw JSON data into
Parquet, runs SQL-based aggregate analyses, and exports tables and figures for a
final report.

## Project Goals

- Explore review volume trends over time.
- Analyze rating distribution and verified purchase behavior.
- Study the relationship between helpful votes, ratings, and review length.
- Compare high-frequency users with ordinary users using the 5-core review set.
- Join review data with product metadata to analyze price buckets and brands.
- Compare keywords in high-rating and low-rating reviews.

## Data Source

Dataset: Amazon product review data, Video Games category.

Local raw files are stored under:

```text
data/reviews_Video_Games.json.gz
data/reviews_Video_Games_5.json.gz
data/meta_Video_Games.json.gz
```

Raw data in HDFS:

```text
hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games.json.gz
hdfs://localhost:9000/user/muxin/amazon_video_games/raw/reviews_Video_Games_5.json.gz
hdfs://localhost:9000/user/muxin/amazon_video_games/raw/meta_Video_Games.json.gz
```

Clean Parquet data in HDFS:

```text
hdfs://localhost:9000/user/muxin/amazon_video_games/clean/reviews_clean
hdfs://localhost:9000/user/muxin/amazon_video_games/clean/reviews5_clean
hdfs://localhost:9000/user/muxin/amazon_video_games/clean/meta_clean
```

Spark output tables in HDFS:

```text
hdfs://localhost:9000/user/muxin/amazon_video_games/outputs/tables
```

## Environment Setup

Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Make sure Hadoop HDFS is running locally at `hdfs://localhost:9000`, and make
sure Spark can connect to it.

## How to Run

Run the full pipeline:

```bash
bash run_all.sh
```

`run_all.sh` uses `.venv/bin/python` automatically when that virtual
environment exists. To use another Python interpreter:

```bash
PYTHON_BIN=/path/to/python bash run_all.sh
```

For local Mac execution, `run_all.sh` also sets conservative default Spark
submit arguments with `local[2]` and 4 GB driver memory unless
`PYSPARK_SUBMIT_ARGS` is already defined.

Or run scripts step by step:

```bash
python3 scripts/01_load_check.py
python3 scripts/02_clean_transform.py
python3 scripts/03_analysis_sql.py
python3 scripts/04_visualization.py
python3 scripts/05_method_depth.py
```

## Script Functions

- `scripts/01_load_check.py`: Reads raw JSON files from HDFS, prints counts,
  schemas, and sample records.
- `scripts/02_clean_transform.py`: Cleans reviews and metadata, creates derived
  fields such as `rating`, `verified`, `helpful_votes`, `review_year`,
  `review_length`, and `price`, then writes clean Parquet data to HDFS.
- `scripts/03_analysis_sql.py`: Reads clean Parquet data, registers Spark SQL
  temporary tables, runs eight analysis questions, writes local CSV files to
  `outputs/tables/`, and writes CSV output directories to HDFS.
- `scripts/04_visualization.py`: Reads local CSV tables from `outputs/tables/`
  and generates PNG charts in `outputs/figures/`.
- `scripts/05_method_depth.py`: Runs a Spark MLlib helpful review prediction
  extension. It labels reviews as helpful when `helpful_votes > 0`, joins
  review features with metadata by `asin`, fills missing `price` with the
  approximate median non-null metadata price, trains a Logistic Regression
  model, and writes model metrics and coefficients.

## SQL Query Reference

The core Spark SQL queries are documented in:

```text
sql/analysis_queries.sql
```

This file extracts the eight analysis queries from `scripts/03_analysis_sql.py`
using the temporary table names `reviews_clean`, `reviews5_clean`, and
`meta_clean`. It is intended as a readable, reproducible experiment reference,
not as a standalone Hive execution script.

## Analysis Output Tables

Local CSV outputs:

```text
outputs/tables/01_yearly_review_trend.csv
outputs/tables/02_rating_distribution.csv
outputs/tables/03_verified_rating_difference.csv
outputs/tables/04_helpful_votes_relationship.csv
outputs/tables/05_user_frequency_behavior_reviews5.csv
outputs/tables/06_price_bucket_rating_reviews.csv
outputs/tables/07_brand_review_rating_ranking.csv
outputs/tables/08_high_low_rating_keywords.csv
outputs/tables/09_helpful_prediction_metrics.csv
outputs/tables/09_helpful_prediction_coefficients.csv
```

The same analysis results are also saved as HDFS CSV directories under:

```text
hdfs://localhost:9000/user/muxin/amazon_video_games/outputs/tables/
```

## Figure Outputs

PNG figures are saved under:

```text
outputs/figures/
```

Expected figures include:

```text
01_yearly_review_count_trend.png
02_rating_distribution.png
03_verified_purchase_avg_rating.png
04_helpful_votes_by_rating_and_length.png
05_user_frequency_rating_behavior.png
06_price_bucket_reviews_and_rating.png
07_top_brands_by_review_count.png
08_high_low_rating_top_keywords.png
08_high_rating_keyword_wordcloud.png
08_low_rating_keyword_wordcloud.png
```

## Dashboard

A static dashboard is available at:

```text
docs/index.html
```

Open it locally from the project root:

```bash
open docs/index.html
```

The dashboard uses copied image assets under `docs/figures/`, so it can be
served directly by GitHub Pages. To deploy, push the repository to GitHub, open
the repository settings, enable GitHub Pages, choose "Deploy from a branch",
select the branch that contains this project, and set the site folder to
`/docs`.
