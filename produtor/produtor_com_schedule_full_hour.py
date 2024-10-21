# Databricks notebook source
pip install schedule

# COMMAND ----------

import time
import schedule
from datetime import datetime, timedelta

# COMMAND ----------

API_KEY= '07d7957df62443d5acf3989ae594f8f9'

# COMMAND ----------

dbutils.widgets.removeAll()
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
            response.raise_for_status()  
            articles = response.json()
            

            requests.post("http://127.0.0.1:5000/webhook", json = articles)
                        
        except requests.exceptions.RequestException as err:
            print(f"Erro na requisição da data {from_date}: {err}")
            raise

def job():
    spark = SparkSession.builder.appName("NewsAPI").getOrCreate()

    to_date_str = to_date_input  
    to_date = datetime.strptime(to_date_str, '%Y-%m-%d') if isinstance(to_date_input, str) else to_date_input

    from_date = to_date - timedelta(days=1)

    news_client = NewsAPIClient(API_KEY)

    news_client.fetch_news(from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))


def run_on_full_hour():
    now = datetime.now()

    next_full_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    time_until_next_hour = (next_full_hour - now).total_seconds()

    time.sleep(time_until_next_hour)

    job()
    schedule.every().hour.at(":00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_on_full_hour()

