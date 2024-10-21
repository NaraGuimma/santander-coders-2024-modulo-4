# Databricks notebook source
pip install deep_translator

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, udf, lower, row_number
from pyspark.sql.window import Window
from datetime import datetime
from deep_translator import GoogleTranslator


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

        # Adiciona uma coluna de número da linha e mantém apenas a primeira (mais recente) por autor e título
        df_deduplicated = df.withColumn("row_num", row_number().over(window_spec)) \
                            .filter(col("row_num") == 1) \
                            .drop("row_num")
        return df_deduplicated

    def save_data(self, df: DataFrame, path: str) -> None:
        """Salva o DataFrame filtrado em formato Delta."""
        df.write.format("delta").mode("overwrite").save(path)


if __name__ == "__main__":
    processor = NewsProcessor()

    df_raw = processor.load_data('dbfs:/FileStore/news_api_raw')

    df_flattened = processor.flatten_df(df_raw)

    df_translated = processor.apply_translation(df_flattened)

    words_list = ['Filogenética','Metagenômica', 'genes', 'DNA']

    filtered_df = processor.filter_content(df_translated, words_list)

    df_deduplicated = processor.remove_duplicates(filtered_df)

    processor.save_data(df_deduplicated, 'dbfs:/FileStore/harmonized')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validando a execução dessa etapa

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/FileStore/harmonized')
df.count()

# COMMAND ----------

df.display()

# COMMAND ----------


