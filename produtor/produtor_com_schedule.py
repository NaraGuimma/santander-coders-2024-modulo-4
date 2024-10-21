# Databricks notebook source
pip install schedule

# COMMAND ----------

import requests
import schedule
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from typing import List, Dict

# COMMAND ----------

API_KEY= '07d7957df62443d5acf3989ae594f8f9'

# COMMAND ----------

# Creating a widget for 'to_date' in Databricks
dbutils.widgets.text("to_date", datetime.today().strftime('%Y-%m-%d'), "Select To Date")
to_date_input = dbutils.widgets.get("to_date")

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
        query = 'genomics OR "personalized medicine research"'
        
        url = f"{self.base_url}?q={query}&from={from_date}&to={to_date}&sortBy=popularity&apiKey={self.api_key}"
    
        try:
            response = requests.get(url)
            response.raise_for_status()  # Lança exceção para status de erro
            articles = response.json()
            

            requests.post("http://127.0.0.1:5000/webhook", json = articles)
                        
        except requests.exceptions.RequestException as err:
            print(f"Erro na requisição da data {from_date}: {err}")
            raise

def job():
    # Initialize the Spark session
    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

    # Get 'to_date' from widget
    to_date_str = to_date_input  # to_date_input comes from the widget
    to_date = datetime.strptime(to_date_str, '%Y-%m-%d') if isinstance(to_date_input, str) else to_date_input

    # Calculate 'from_date' as one day before 'to_date'
    from_date = to_date - timedelta(days=1)

    news_client = NewsAPIClient(API_KEY)

    # Process news articles for the given date range
    news_client.fetch_news(from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    # Schedule the job to run every hour
    schedule.every().hour.do(job)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

