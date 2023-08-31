import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import sum, count, lit, min, max, stddev
import ast

conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key', 'AKIAXUOLOK26QSESY4T3')
conf.set('spark.hadoop.fs.s3a.secret.key',
         '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
         'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


def overall_analysis(pl_df):
    pl_1993 = pl_df.filter((pl_df.season == 1993) & (
        pl_df.lose >= 20)).select(pl_df.team)
    print("Teams that Lost 20+ Matches in 1993")
    pl_1993.show()

    team_distinct = pl_df.select(pl_df.team).distinct().count()
    print(team_distinct, " Distinct Teams Have Played in the EPL")

    avg_goals = pl_df.groupBy(pl_df.team).agg(
        (sum(pl_df.goal_for) / count(lit(1))).alias("average_goals")).sort("average_goals", ascending=True)
    least_goals = avg_goals.head(10)
    most_goals = avg_goals.tail(10)

    print("The top 10 teams that has scored the least average goals")
    print(least_goals)
    print("The top 10 teams that has scored the highest average goals")
    print(most_goals)

    avg_conceded = pl_df.groupBy(pl_df.team).agg(
        (sum(pl_df.goal_against) / count(lit(1))).alias("average_conceded")).sort("average_conceded", ascending=True)
    least_conceded = avg_conceded.head(10)
    most_conceded = avg_conceded.tail(10)

    print("The top 10 teams that has conceded the least average goals")
    print(least_conceded)
    print("The top 10 teams that has conceded the most average goals")
    print(most_conceded)

    draws = pl_df.groupBy(pl_df.team).agg(sum(pl_df.draw).alias(
        "total_draws")).sort("total_draws", ascending=False)

    print("Teams with the most draws")
    draws.show()

    titles = pl_df.filter(pl_df.rank == 1).groupBy(
        pl_df.team).count().withColumnRenamed('count', 'titles').sort("titles", ascending=False)
    print("Teams with the most PL titles")
    titles.show()

    goal_ratio = pl_df.withColumn(
        "goal_ratio", pl_df.goal_for / pl_df.goal_against).sort("goal_ratio", ascending=False).select(pl_df.season, pl_df.team, "goal_ratio")

    print("Teams with the best goal ratio over all season")
    goal_ratio.show(10)


def temporal_analysis(pl_df):
    end_year = 2023
    start_year = end_year - 4
    while True:
        if start_year < 1993:
            break

        period_df = pl_df.filter(
            (pl_df.season >= start_year) & (pl_df.season <= end_year))

        # What does the term count(lit(1)) mean?
        period_summary = period_df.groupby(period_df.team).agg(count(lit(1)).alias("PL Seasons"),
                                                               sum(period_df.goal_for).alias("total_goals_scored"),  sum(period_df.goal_against).alias("total_goals_conceded"), stddev(period_df.goal_for).alias("std_goals_scored"), stddev(period_df.goal_against).alias("std_goals_conceded"), min(period_df.points).alias("min_points"), max(period_df.points).alias("max_points"), sum(period_df.points).alias("total_points")).sort(period_df.team, ascending=True).fillna(0)

        file_name = "pl_data_" + str(start_year) + "_" + str(end_year)
        period_summary.show()
        period_summary.write.format("csv").option("header", True).save(
            "s3a://iris-data-tanatip/output_folder/" + file_name + ".csv", mode="overwrite")
        end_year = start_year - 1
        start_year -= 5


conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key', 'AKIAXUOLOK26QSESY4T3')
conf.set('spark.hadoop.fs.s3a.secret.key',
         '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
         'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

spark = SparkSession.builder.config(conf=conf).appName(
    "transform_data").getOrCreate()

location = "s3a://iris-data-tanatip/input_folder/premier-league-tables.txt"

lines = spark.sparkContext.textFile(location)
lines = lines.map(lambda x: ast.literal_eval(x))

df = spark.createDataFrame(lines)
df = df.drop(df.notes)
df.show()

overall_analysis(df)
temporal_analysis(df)

spark.stop()
