# Databricks notebook source
pip install deep_translator

# COMMAND ----------

pip install schedule

# COMMAND ----------

import requests
from pyspark.sql.functions import * 
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
import json
from functools import reduce
import os
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from deep_translator import GoogleTranslator
import schedule
import time
import pytz
from pyspark.sql.functions import year, month, dayofmonth, count, col, lower, col, udf, lower, row_number
from pyspark.sql import functions as F


# COMMAND ----------

class NewsProcessor:
    def __init__(self) -> None:
        """Inicializa a sessão Spark e define o esquema de dados."""
        self.spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

    def load_data(self, path: str) -> DataFrame:
        """Carrega os dados de uma fonte Delta."""
        df = self.spark.read.format('delta').load(path)
        return df

    def flatten_df(self, df: DataFrame) -> DataFrame:
        """Achata o campo 'source' e cria novas colunas 'source_id' e 'source_name'."""
        df_flattened = df \
            .withColumn("source_id", col("source.id")) \
            .withColumn("source_name", col("source.name")) \
            .drop("source")
        return df_flattened

    @staticmethod
    def translate_to_portuguese(text: str) -> str:
        """Traduz um texto para o português brasileiro usando GoogleTranslator."""
        try:
            if text is None:
                return None
            return GoogleTranslator(source='auto', target='pt').translate(text)
        except Exception as e:
            return str(e)

    def apply_translation(self, df: DataFrame) -> DataFrame:
        """Aplica a tradução para português nas colunas 'description', 'content' e 'title'."""
        translate_udf = udf(self.translate_to_portuguese, StringType())

        df_translated = df \
            .withColumn("description_pt", translate_udf(col("description"))) \
            .withColumn("content_pt", translate_udf(col("content"))) \
            .withColumn("title_pt", translate_udf(col("title")))
        
        return df_translated

    def filter_content(self, df: DataFrame, words_list: list[str]) -> DataFrame:
        """Filtra o conteúdo nas colunas 'description_pt', 'content_pt' e 'title_pt' com base em uma lista de palavras."""
        words_list = [word.lower() for word in words_list]
        words_pattern = '|'.join(words_list)

        filter_condition = (
            lower(col('description_pt')).rlike(words_pattern) |
            lower(col('content_pt')).rlike(words_pattern) |
            lower(col('title_pt')).rlike(words_pattern)
        )

        filtered_df = df.filter(filter_condition)
        return filtered_df

    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicados com base em 'author' e 'title', mantendo o registro mais recente com base em 'publishedAt'."""
        window_spec = Window.partitionBy("author", "title").orderBy(col("publishedAt").desc())

        df_deduplicated = df.withColumn("row_num", row_number().over(window_spec)) \
                            .filter(col("row_num") == 1) \
                            .drop("row_num")
        return df_deduplicated

    def save_data(self, df: DataFrame, path: str) -> None:
        """Salva o DataFrame filtrado em formato Delta."""
        df.write.format("delta").mode("overwrite").save(path)


# COMMAND ----------

class NewsAggregator:
    
    def __init__(self, df: DataFrame) -> None:
        """
        Inicializa a classe com o DataFrame transformado que será utilizado para as agregações.
        """
        self.df = df
    
    def count_by_publication_date(self) -> DataFrame:
        """
        Conta a quantidade de notícias por ano, mês e dia de publicação.
        """
        df_by_date = self.df \
            .withColumn("year", year(col("publishedAt"))) \
            .withColumn("month", month(col("publishedAt"))) \
            .withColumn("day", dayofmonth(col("publishedAt"))) \
            .groupBy("year", "month", "day") \
            .agg(count("*").alias("news_count"))
        
        df_by_date.write.format("delta").mode("overwrite").save("dbfs:/FileStore/semantic/year_month_day_aggregation")

    def count_by_source_and_author(self) -> DataFrame:
        """
        Conta a quantidade de notícias por fonte e autor.
        """
        df_by_source_author = self.df \
            .groupBy("source_name", "author") \
            .agg(count("*").alias("news_count"))
        
        df_by_source_author.write.format("delta").mode("overwrite").save("dbfs:/FileStore/semantic/source_author_aggregation")

    def count_keyword_appearances(self, keywords: list[str]) -> DataFrame:
        """
        Conta a quantidade de aparições de palavras-chave por ano, mês e dia de publicação.
        """
        keywords_pattern = '|'.join([kw.lower() for kw in keywords])

        df_keywords = self.df \
            .withColumn("year", year(col("publishedAt"))) \
            .withColumn("month", month(col("publishedAt"))) \
            .withColumn("day", dayofmonth(col("publishedAt"))) \
            .withColumn("keyword_match", F.expr(f"""
                lower(description_pt) rlike '{keywords_pattern}' OR
                lower(content_pt) rlike '{keywords_pattern}' OR
                lower(title_pt) rlike '{keywords_pattern}'
            """)) \
            .filter(col("keyword_match") == True) \
            .groupBy("year", "month", "day") \
            .agg(count("*").alias("keyword_appearance_count"))
        
        df_keywords.write.format("delta").mode("overwrite").save("dbfs:/FileStore/semantic/keyword_appearance_aggregation")

# COMMAND ----------

def news_processor():
    processor = NewsProcessor()
    df_raw = processor.load_data('dbfs:/FileStore/news_api_raw')
    df_flattened = processor.flatten_df(df_raw)
    df_translated = processor.apply_translation(df_flattened)
    words_list = ['Filogenética', 'Metagenômica', 'genes', 'DNA']
    filtered_df = processor.filter_content(df_translated, words_list)
    df_deduplicated = processor.remove_duplicates(filtered_df)
    processor.save_data(df_deduplicated, 'dbfs:/FileStore/harmonized')
    

def news_aggregator():
    df_harmonized = spark.read.format('delta').load('dbfs:/FileStore/harmonized')
    keywords = ['filogenética', 'metagenômica', 'genes', 'dna']
    aggregator = NewsAggregator(df_harmonized)
    aggregator.count_by_publication_date()
    aggregator.count_by_source_and_author()
    aggregator.count_keyword_appearances(keywords)

def job():
    news_processor()
    print("Dados harmonizados com sucesso")
    news_aggregator()
    print("Dados agregados com sucesso")


def run_daily_at_8_pm_brasilia():
    brt_tz = pytz.timezone('America/Sao_Paulo')

    now = datetime.now(brt_tz)
    
    scheduled_time = now.replace(hour=20, minute=0, second=0, microsecond=0)

    if now > scheduled_time:
        scheduled_time += timedelta(days=1)

    time_until_scheduled = (scheduled_time - now).total_seconds()

    Sleep until the scheduled time
    time.sleep(time_until_scheduled)

    Run the job and schedule it daily at 8 PM Brasília time
    job()
    schedule.every().day.at("20:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_daily_at_8_pm_brasilia()


# COMMAND ----------


