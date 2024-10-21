# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, count, col, lower
from pyspark.sql import functions as F

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
        return df_by_date

    def count_by_source_and_author(self) -> DataFrame:
        """
        Conta a quantidade de notícias por fonte e autor.
        """
        df_by_source_author = self.df \
            .groupBy("source_name", "author") \
            .agg(count("*").alias("news_count"))
        
        df_by_source_author.write.format("delta").mode("overwrite").save("dbfs:/FileStore/semantic/source_author_aggregation")
        return df_by_source_author

    def count_keyword_appearances(self, keywords: list[str]) -> DataFrame:
        """
        Conta a quantidade de aparições de palavras-chave por ano, mês e dia de publicação.
        """
        # Unindo as palavras-chave em um padrão de expressão regular
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
        return df_keywords


if __name__ == "__main__":

    df_harmonized = spark.read.format('delta').load('dbfs:/FileStore/harmonized')
    keywords = ['filogenética', 'metaalgumacoisa', 'metagenômica', 'genes', 'dna']

    aggregator = NewsAggregator(df_harmonized)

    aggregator.count_by_publication_date()

    aggregator.count_by_source_and_author()

    aggregator.count_keyword_appearances(keywords)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Validando as agregaões

# COMMAND ----------

year_month_day_aggregation = spark.read.format('delta').load("dbfs:/FileStore/semantic/year_month_day_aggregation")

year_month_day_aggregation.display()

# COMMAND ----------

source_author_aggregation = spark.read.format('delta').load("dbfs:/FileStore/semantic/source_author_aggregation")
source_author_aggregation.display()

# COMMAND ----------

keyword_appearance_aggregation = spark.read.format('delta').load("dbfs:/FileStore/semantic/keyword_appearance_aggregation")
keyword_appearance_aggregation.display()
