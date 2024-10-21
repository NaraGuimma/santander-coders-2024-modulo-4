# Databricks notebook source
pip install schedule

# COMMAND ----------

import requests 
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import List, Dict
import time
import schedule

# COMMAND ----------

# MAGIC %run ./env

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("to_date", datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "Select To Date")
to_date_input = dbutils.widgets.get("to_date")
to_date_input

# COMMAND ----------

# Outra forma de criar os widgets
# import ipywidgets as widgets
# from IPython.display import display

# to_date_picker = widgets.DatePicker(
#     description='Select To Date',
#     value=datetime.today()
# )
# display(to_date_picker)

# to_date_input = to_date_picker.value
# to_date_input

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
        query = 'genomics OR "personalized medicine research"'
        
        url = f"{self.base_url}?q={query}&from={from_date}&to={to_date}&sortBy=popularity&apiKey={self.api_key}"
    
        try:
            response = requests.get(url)
            response.raise_for_status()  
            articles = response.json()
            print(articles)
            

            requests.post("http://127.0.0.1:5008/webhook", json = articles)
                        
        except requests.exceptions.RequestException as err:
            print(f"Erro na requisição da data {from_date}: {err}")
            raise

# COMMAND ----------

def main():
    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

    to_date = to_date_input 

    from_date = datetime.now() - timedelta(days=10)

    news_client = NewsAPIClient(API_KEY)

    news_client.fetch_news(from_date.strftime('%Y-%m-%dT%H:%M:%S'), to_date)

if __name__ == "__main__":
    main()


# COMMAND ----------


