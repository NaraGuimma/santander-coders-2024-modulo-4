# Sistema de Monitoramento de Avanços no Campo da Genômica

## Descrição do Projeto

Este projeto foi desenvolvido simulando um time de engenharia de dados da empresa ficticia HealthGen, uma empresa especializada em genômica e medicina personalizada. A genômica, que estuda o conjunto completo de genes de um organismo, desempenha um papel crucial na pesquisa biomédica e medicina personalizada, permitindo a identificação de variantes genéticas e mutações associadas a doenças, além de facilitar tratamentos com base nas características genéticas dos pacientes.

O objetivo principal deste projeto foi criar um sistema que coleta, analisa e apresenta as últimas notícias relacionadas ao campo da genômica e medicina personalizada, permitindo à empresa se manter atualizada com os avanços recentes, identificar novas oportunidades de pesquisa e otimizar suas estratégias de desenvolvimento.

## Funcionalidades do Sistema

O sistema foi desenhado para executar várias atividades essenciais:

1. **Consumo de dados com a News API:**
   - Utilizamos a News API (https://newsapi.org/) para buscar notícias de fontes confiáveis e especializadas sobre genômica e medicina personalizada.
   - O sistema extrai notícias relevantes que abordam temas como sequenciamento de DNA, terapias genéticas personalizadas e descobertas sobre doenças genéticas específicas.

2. **Definição de Critérios de Relevância:**
   - Implementamos filtros para garantir que apenas notícias com impacto significativo sobre o campo sejam consideradas. Essas notícias incluem palavras-chave relacionadas a genômica, sequenciamento de DNA, terapias genéticas e doenças específicas.
   - As palavras escolhidas foram: **Filogenética, Metagenômica, Genes, DNA**

3. **Cargas em Batches:**
   - As notícias relevantes são armazenadas em um banco de dados, onde realizamos a carga em batches a cada hora.
   - O processo evita a duplicação de dados, garantindo que uma notícia não seja armazenada mais de uma vez.

4. **Transformação de Dados para Consulta Pública:**
   - Aplicamos transformações diárias nos dados, permitindo consultas avançadas com base nos seguintes critérios:
     - Quantidade de notícias por ano, mês e dia de publicação.
     - Quantidade de notícias por fonte e autor.
     - Quantidade de aparições de palavras-chave relevantes por ano, mês e dia.

5. **Eventos em Tempo Real:**
   O sistema também oferece a capacidade de lidar com dados em tempo real, por meio de duas opções:

   - **Opção 1 - Apache Kafka e Spark Streaming:**
     - Implementamos um pipeline de dados usando Apache Kafka e Spark Streaming, capaz de consumir eventos em tempo real e armazenar dados temporariamente para uma verificação paralela.
     - Após a verificação, os dados são inseridos no destino principal de armazenamento, evitando duplicações.
    
<img width="1083" alt="image" src="https://github.com/user-attachments/assets/a6fd52f7-27c1-40bf-b00a-ddd728451a58">

<p align="center">Desenho da solução usando Kafka</p>


   - **Opção 2 - Webhooks com Notificações por Eventos:**
     - Configuramos um webhook para receber dados a partir de um evento representado por uma requisição POST. As notícias adquiridas por esse método são armazenadas temporariamente e verificadas antes de serem inseridas no destino final, evitando dados duplicados.

<img width="923" alt="image" src="https://github.com/user-attachments/assets/ecc2c71f-79ed-4da8-b3b5-1659cb1f64c4">


<p align="center">Desenho da solução usando Webhook</p>



6. **Orquestração e Harmonização:**
   - Utilizamos o Prefect para orquestrar o fluxo de dados, garantindo que as agregações e transformações sejam atualizadas diariamente.
   - As etapas de harmonização incluem filtragem pelos critérios de relevância, remoção de dados duplicados, tradução para o portugues das colunas content, title, e description, e aplicação de transformações semânticas para gerar as agregações finais.

## Tecnologias Utilizadas

### 1. **Webhooks:**
   - Webhooks são mecanismos que permitem que uma aplicação envie dados para outra aplicação em tempo real, por meio de requisições HTTP. No projeto, usamos webhooks para capturar eventos e realizar chamadas à News API.

### 2. **Apache Kafka:**
   - Kafka é uma plataforma de streaming distribuída que permite a publicação, armazenamento e consumo de fluxos de dados em tempo real. Utilizamos Kafka para capturar eventos em tempo real, possibilitando a ingestão contínua de dados de notícias.

### 3. **Prefect:**
   - Prefect é uma plataforma de orquestração de fluxos de trabalho que facilita a execução programada de pipelines de dados. Usamos Prefect para garantir a execução diária das transformações e agregações de dados.

### 4. **ETL (Extract, Transform, Load):**
   - ETL é um processo de pipeline de dados que envolve a extração de dados de uma ou mais fontes, a transformação dos dados para atender a critérios específicos (como limpeza e agregação), e a carga dos dados transformados em um sistema de destino.
   - Este projeto implementa um pipeline ETL robusto para extrair notícias, aplicar critérios de relevância, filtrar duplicações e transformar os dados para consulta pública.

### 5. **Apache Spark:**
   - Spark é uma poderosa ferramenta de processamento distribuído de grandes volumes de dados. Neste projeto, utilizamos o Spark Streaming para processar dados de eventos em tempo real provenientes do Apache Kafka.

### 6. **Databricks e Delta Lake:**
   - Databricks é uma plataforma que combina Apache Spark com ferramentas colaborativas para facilitar o processamento e análise de dados em escala.
   - Delta Lake é uma camada de armazenamento otimizada que acrescenta confiabilidade e desempenho ao processamento de dados. Utilizamos o Delta Lake para armazenar e gerenciar nossos dados de forma segura, garantindo a integridade dos dados ao longo do pipeline.

## Como Executar o Projeto

1. **Configuração da News API:**
   - Acesse https://newsapi.org/ e obtenha sua chave de API.
   - Configure a chave no arquivo de variávem de ambiente `env`.

2. **Instalação das Dependências:**
   - Execute o seguinte comando para instalar todas as dependências:
     ```bash
     pip install -r requirements.txt
     ```

## Conclusão

Este sistema oferece uma solução completa para o monitoramento de avanços no campo da genômica, garantindo que as notícias e descobertas mais relevantes sejam processadas, filtradas e disponibilizadas para análise. A combinação de pipelines de dados, eventos em tempo real e orquestração automática permite à nossa empresa fictícia, HealthGen, se manter na vanguarda da pesquisa em medicina personalizada.
