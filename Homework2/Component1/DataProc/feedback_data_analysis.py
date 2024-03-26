from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, explode, split, lower, col, collect_list, format_number, count, regexp_replace
import json

# Start session
spark = SparkSession.builder.appName("FeedbackAnalysis").getOrCreate()

# Read the CSV file
feedback_df = spark.read.option("header", "true").csv("gs://feedback_data_bucket/feedback_export.csv") \
    .withColumnRenamed("Restaurant Id", "id") \
    .withColumnRenamed("Restaurant Name", "name") \
    .withColumnRenamed("Restaurant Location", "location")

# Average ratings for each restaurant
def format_avg(column):
    return format_number(avg(column), 1).alias(f"avg{column.split()[0]}")

avg_df = feedback_df.groupBy("id", "name", "location") \
    .agg(
        format_avg("Ambiance"),
        format_avg("Food Quality"),
        format_avg("Service"),
        format_number(((avg("Ambiance") + avg("Food Quality") + avg("Service")) / 3), 1).alias("avgRating"),
        count("id").alias("revCount")
    )

# Common keywords -more than 2 occurrences- in comments, excluding stopwords and punctuation
stopwords = ["and", "the", "a", "to", "of", "in", "on", "for", "with", "at", "by", "from", "up", "out", "as", "so", "it", "be", "is", "are", "that", "was", "were", "will", "or", "an", 'i']

feedback_df = feedback_df.withColumn("Comment", regexp_replace(col("Comment"), "[!?.]", ""))

keywords_df = feedback_df.withColumn("word", explode(split(lower(col("Comment")), "\\s+"))) \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("id", "word") \
    .count() \
    .filter(col("count") > 2) \
    .groupBy("id") \
    .agg(collect_list(col("word")).alias("keywords"))

result_df = avg_df.join(keywords_df, "id", "left")
print("Analysis created: " , result_df.show())

# Write the result to a JSON file in a bucket
result_df.coalesce(1).write.mode("overwrite").json("gs://feedback_data_bucket/restaurant_feedback_analysis.json")

spark.stop()