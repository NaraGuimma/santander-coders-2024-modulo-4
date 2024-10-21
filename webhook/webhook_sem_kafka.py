# Databricks notebook source
pip install flask

# COMMAND ----------

from flask import Flask, request
from pyspark.sql import SparkSession, DataFrame
import json
from typing import List, Dict
from datetime import datetime, timedelta
from pyspark.sql.functions import lit

# COMMAND ----------

class NewsDataProcessor:
    """
    Classe responsável por processar e salvar os dados de notícias utilizando Spark.

    Atributos:
    -----------
    spark : SparkSession
        Sessão do Spark para processamento de dados.

    Métodos:
    --------
    convert_to_dataframe(articles: List[Dict]) -> DataFrame:
        Converte uma lista de artigos para um DataFrame do Spark.

    save_to_delta(df: DataFrame, from_date: str):
        Salva o DataFrame no formato Delta no DBFS, particionado pela data de 'from_date'.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.output_path = "/FileStore/news_api_raw"
    
    def convert_to_dataframe(self, articles: List[Dict]) -> DataFrame:
        """
        Converte uma lista de artigos em um DataFrame do Spark.

        Parâmetros:
        -----------
        articles : List[Dict]
            Lista de dicionários contendo os artigos.

        Retorna:
        --------
        DataFrame:
            DataFrame do Spark com os artigos.
        """
        return self.spark.createDataFrame(articles)
    
    def save_to_delta(self, df: DataFrame, from_date: str):
        """
        Salva o DataFrame no formato Delta no DBFS, particionado pela data 'from_date'.

        Parâmetros:
        -----------
        df : DataFrame
            DataFrame do Spark a ser salvo.

        from_date : str
            Data para a qual os dados serão particionados no formato 'YYYY-MM-DD'.
        """
        df.write \
          .format('delta') \
          .mode("append") \
          .partitionBy("from_date") \
          .save(self.output_path)  
        print(f"Dados salvos com sucesso em {self.output_path}")

# COMMAND ----------

app = Flask("app")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json 
    articles = data.get("articles", [])
    
    if not articles:
        return "Nenhum artigo encontrado", 400

    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()
    processor = NewsDataProcessor(spark)

    df_concat = None
    from_date = datetime.now()

    if articles:
        df = processor.convert_to_dataframe(articles)
        df = df.withColumn('from_date', lit(from_date.strftime('%Y-%m-%d')))

        if df_concat is None:
            df_concat = df
        else:
            df_concat = df_concat.unionByName(df)

    if df_concat is not None:
        processor.save_to_delta(df_concat, from_date.strftime('%Y-%m-%d'))

    return 'Webhook OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5008)

# COMMAND ----------


