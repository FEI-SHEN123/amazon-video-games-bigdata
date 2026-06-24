from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from wordcloud import WordCloud


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE_DIR = PROJECT_ROOT / "outputs" / "tables"
FIGURE_DIR = PROJECT_ROOT / "outputs" / "figures"


def read_table(name):
    path = TABLE_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing input table: {path}")
    return pd.read_csv(path)


def save_figure(filename):
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURE_DIR / filename
    plt.tight_layout()
    plt.savefig(path, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved figure: {path}")


def title_case_label(value):
    return str(value).replace("_", " ").title()


def plot_yearly_review_trend():
    df = read_table("01_yearly_review_trend")
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=df, x="review_year", y="review_count", marker="o", linewidth=2)
    plt.title("Yearly Review Count Trend")
    plt.xlabel("Review Year")
    plt.ylabel("Number of Reviews")
    plt.grid(axis="y", alpha=0.25)
    save_figure("01_yearly_review_count_trend.png")


def plot_rating_distribution():
    df = read_table("02_rating_distribution")
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=df, x="rating", y="review_count", color="#4C78A8")
    ax.bar_label(ax.containers[0], fmt="%.0f", padding=3, fontsize=9)
    plt.title("Rating Distribution")
    plt.xlabel("Rating")
    plt.ylabel("Number of Reviews")
    save_figure("02_rating_distribution.png")


def plot_verified_rating_difference():
    df = read_table("03_verified_rating_difference")
    df["purchase_label"] = df["purchase_type"].map(title_case_label)
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=df, x="purchase_label", y="avg_rating", color="#59A14F")
    ax.set_ylim(0, 5)
    ax.bar_label(ax.containers[0], fmt="%.2f", padding=3, fontsize=10)
    plt.title("Average Rating by Verified Purchase Status")
    plt.xlabel("Purchase Type")
    plt.ylabel("Average Rating")
    save_figure("03_verified_purchase_avg_rating.png")


def plot_helpful_votes_relationship():
    df = read_table("04_helpful_votes_relationship")
    ordered_columns = (
        df[["review_length_bucket_order", "review_length_bucket"]]
        .drop_duplicates()
        .sort_values("review_length_bucket_order")["review_length_bucket"]
        .tolist()
    )
    pivot = df.pivot_table(
        index="rating",
        columns="review_length_bucket",
        values="avg_helpful_votes",
        aggfunc="mean",
    )
    pivot = pivot.reindex(columns=ordered_columns)

    plt.figure(figsize=(10, 5.5))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="YlGnBu", linewidths=0.4)
    plt.title("Average Helpful Votes by Rating and Review Length")
    plt.xlabel("Review Length Bucket")
    plt.ylabel("Rating")
    save_figure("04_helpful_votes_by_rating_and_length.png")


def plot_user_frequency_behavior():
    df = read_table("05_user_frequency_behavior_reviews5")
    df["user_group_label"] = df["user_group"].map(title_case_label)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    sns.barplot(data=df, x="user_group_label", y="avg_rating", color="#F28E2B", ax=axes[0])
    axes[0].set_ylim(0, 5)
    axes[0].set_title("Average Rating")
    axes[0].set_xlabel("User Group")
    axes[0].set_ylabel("Average Rating")
    axes[0].bar_label(axes[0].containers[0], fmt="%.2f", padding=3, fontsize=9)

    sns.barplot(data=df, x="user_group_label", y="positive_review_pct", color="#76B7B2", ax=axes[1])
    axes[1].set_ylim(0, 100)
    axes[1].set_title("Positive Review Share")
    axes[1].set_xlabel("User Group")
    axes[1].set_ylabel("Reviews Rated 4 or 5 (%)")
    axes[1].bar_label(axes[1].containers[0], fmt="%.1f%%", padding=3, fontsize=9)

    fig.suptitle("High-Frequency vs Ordinary User Rating Behavior", y=1.02)
    save_figure("05_user_frequency_rating_behavior.png")


def plot_price_bucket_relationship():
    df = read_table("06_price_bucket_rating_reviews").sort_values("price_bucket_order")
    fig, ax1 = plt.subplots(figsize=(10, 5.5))
    sns.barplot(data=df, x="price_bucket", y="review_count", color="#4E79A7", ax=ax1)
    ax1.set_xlabel("Price Bucket")
    ax1.set_ylabel("Number of Reviews")
    ax1.tick_params(axis="x", rotation=25)

    ax2 = ax1.twinx()
    sns.lineplot(data=df, x="price_bucket", y="avg_rating", marker="o", color="#E15759", ax=ax2)
    ax2.set_ylim(0, 5)
    ax2.set_ylabel("Average Rating")

    plt.title("Review Volume and Average Rating by Product Price Bucket")
    save_figure("06_price_bucket_reviews_and_rating.png")


def plot_brand_ranking():
    df = read_table("07_brand_review_rating_ranking").head(15).copy()
    df["brand_label"] = df["brand"].astype(str).str.slice(0, 45)

    plt.figure(figsize=(10, 7))
    sns.barplot(data=df, y="brand_label", x="review_count", hue="avg_rating", palette="viridis", dodge=False)
    plt.title("Top Brands by Review Count")
    plt.xlabel("Number of Reviews")
    plt.ylabel("Brand")
    plt.legend(title="Avg Rating", loc="lower right")
    save_figure("07_top_brands_by_review_count.png")


def plot_keywords():
    df = read_table("08_high_low_rating_keywords")
    top_df = df[df["keyword_rank"] <= 15].copy()
    top_df["sentiment_label"] = top_df["sentiment_group"].map(title_case_label)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharex=False)
    for ax, group in zip(axes, ["high_rating", "low_rating"]):
        part = top_df[top_df["sentiment_group"] == group].sort_values("keyword_count", ascending=True)
        sns.barplot(data=part, y="keyword", x="keyword_count", color="#9C755F", ax=ax)
        ax.set_title(title_case_label(group))
        ax.set_xlabel("Keyword Count")
        ax.set_ylabel("Keyword")

    fig.suptitle("Top Keywords in High-Rating and Low-Rating Reviews", y=1.02)
    save_figure("08_high_low_rating_top_keywords.png")

    for group in ["high_rating", "low_rating"]:
        part = df[df["sentiment_group"] == group]
        frequencies = dict(zip(part["keyword"], part["keyword_count"]))
        if not frequencies:
            continue

        cloud = WordCloud(
            width=1200,
            height=700,
            background_color="white",
            colormap="tab10",
            random_state=42,
        ).generate_from_frequencies(frequencies)

        plt.figure(figsize=(10, 6))
        plt.imshow(cloud, interpolation="bilinear")
        plt.axis("off")
        plt.title(f"{title_case_label(group)} Keyword Word Cloud")
        save_figure(f"08_{group}_keyword_wordcloud.png")


def main():
    sns.set_theme(style="whitegrid", context="notebook")
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    plot_yearly_review_trend()
    plot_rating_distribution()
    plot_verified_rating_difference()
    plot_helpful_votes_relationship()
    plot_user_frequency_behavior()
    plot_price_bucket_relationship()
    plot_brand_ranking()
    plot_keywords()

    print("All figures saved.")


if __name__ == "__main__":
    main()
