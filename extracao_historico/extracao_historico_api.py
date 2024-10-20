# Databricks notebook source
!pip install -r 

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, StructType
import os

# COMMAND ----------

# MAGIC %run ./env

# COMMAND ----------

# exclui recursivamente todo o conteudo de uma pasta
# dbutils.fs.rm('/dbfs/FileStore/bronze_layer', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraindo os dados e salvando em delta a cada extração

# COMMAND ----------

class NewsAPIClient:
    """
    Classe responsável por fazer requisições à API do NewsAPI e retornar os resultados.
    
    Atributos:
    -----------
    api_key : str
        Chave da API para autenticação no NewsAPI.
    
    Métodos:
    --------
    fetch_news(from_date: str) -> List[Dict]:
        Faz a requisição à API do NewsAPI e retorna uma lista de artigos.
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
        
    def fetch_news(self, from_date: str, to_date: str) -> List[Dict]:
        """
        Faz a requisição para a API NewsAPI com base na data fornecida e retorna uma lista de artigos.

        Parâmetros:
        -----------
        from_date : str
            Data a partir da qual buscar as notícias, no formato 'YYYY-MM-DD'.

        Retorna:
        --------
        List[Dict]:
            Lista de dicionários contendo os artigos retornados pela API.
        """
        all_articles = []
        page = 1
        query = 'genomics OR "personalized medicine research"'
        
        while page <= 6:

            url = f"{self.base_url}?q={query}&from={from_date}&to={to_date}&page={page}&sortBy=popularity&apiKey={self.api_key}"
        
            try:
                response = requests.get(url)
                response.raise_for_status() 
                articles = response.json().get("articles", [])
                
                if not articles:
                    break  
                
                all_articles.extend(articles) 
                page += 1 
                
            except requests.exceptions.RequestException as err:
                print(f"Erro na requisição da data {from_date} na página {page}: {err}")
                break
        
        return all_articles

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
        self.output_path = f"/dbfs/FileStore/bronze_layer"
    
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
        df = df.withColumn('from_date', lit(from_date.strftime('%Y-%m-%d'))) 
        df.write \
          .format('delta') \
          .mode("overwrite") \
          .partitionBy("from_date") \
          .save(self.output_path)
        print(f"Dados salvos com sucesso em {from_date}")

def main():
    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

    news_client = NewsAPIClient(API_KEY)
    processor = NewsDataProcessor(spark)

    # Calcula a data de um mês atrás
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    from_date = one_month_ago
    
    while from_date <= today:
        to_date = from_date + timedelta(days=1)

        articles = news_client.fetch_news(from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))


        if articles:
            df = processor.convert_to_dataframe(articles)
            processor.save_to_delta(df, from_date)

        from_date = from_date + timedelta(days=1)

if __name__ == "__main__":
    main()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validando se foi salvo com sucesso

# COMMAND ----------

spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

# COMMAND ----------

dbutils.fs.ls('/dbfs/FileStore/bronze_layer')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraindo todos os dados e salvando em delta após toda a extração

# COMMAND ----------

class NewsAPIClient:
    """
    Classe responsável por fazer requisições à API do NewsAPI e retornar os resultados.
    
    Atributos:
    -----------
    api_key : str
        Chave da API para autenticação no NewsAPI.
    
    Métodos:
    --------
    fetch_news(from_date: str, to_date: str) -> List[Dict]:
        Faz a requisição à API do NewsAPI e retorna uma lista de artigos.
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
        
    def fetch_news(self, from_date: str, to_date: str) -> List[Dict]:
        """
        Faz a requisição para a API NewsAPI com base na data fornecida e retorna uma lista de artigos.

        Parâmetros:
        -----------
        from_date : str
            Data a partir da qual buscar as notícias, no formato 'YYYY-MM-DD'.

        to_date : str
            Data até a qual buscar as notícias, no formato 'YYYY-MM-DD'.

        Retorna:
        --------
        List[Dict]:
            Lista de dicionários contendo os artigos retornados pela API.
        """
        all_articles = []
        page = 1
        query = 'genomics OR "personalized medicine research"'
        
        while page <= 6:

            url = f"{self.base_url}?q={query}&from={from_date}&to={to_date}&page={page}&sortBy=popularity&apiKey={self.api_key}"
        
            try:
                response = requests.get(url)
                response.raise_for_status()  
                articles = response.json().get("articles", [])
                
                if not articles:
                    break  
                
                all_articles.extend(articles)  
                page += 1 
                
            except requests.exceptions.RequestException as err:
                print(f"Erro na requisição da data {from_date} na página {page}: {err}")
                break
        
        return all_articles

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
          .mode("overwrite") \
          .partitionBy("from_date") \
          .save(self.output_path)  
        print(f"Dados salvos com sucesso em {self.output_path}")

def main():
    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

    news_client = NewsAPIClient(API_KEY)
    processor = NewsDataProcessor(spark)

    # Calcula a data de um mês atrás
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    from_date = one_month_ago

    df_concat = None  

    while from_date <= today:
        to_date = from_date + timedelta(days=1)

        articles = news_client.fetch_news(from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

        if articles:
            df = processor.convert_to_dataframe(articles)
            df = df.withColumn('from_date', lit(from_date.strftime('%Y-%m-%d')))  
            
            if df_concat is None:
                df_concat = df
            else:
                df_concat = df_concat.unionByName(df)

        from_date = from_date + timedelta(days=1)

    if df_concat is not None:
        processor.save_to_delta(df_concat, from_date.strftime('%Y-%m-%d'))

if __name__ == "__main__":
    main()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validando se foi salvo com sucesso

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/news_api_raw')

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/FileStore/news_api_raw')
df.count()
