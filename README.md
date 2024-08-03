# Pipeline de Dados

# Pipeline de Dados

https://github.com/thiago-vale/pipeline_de_dados

## Sobre o Projeto

Este projeto consiste no desenvolvimento de pipelines de dados para um Data Lake.

## Estrutura das pastas
```
.
├── dags # Orquestração dos Pipelines com as Dags do Airflow
├── data # Dados Crus para testes do Projeto
├── notebboks # Notebooks para Testes de Analises do Data Frame
├── src # Codigos do Pipelines das camadas do Data Lake
└── utils # Configurações do pyspark e Classes construidas para o projeto
    └── spark_jars # .jars para conexões 
```

## Tecnologias Utilizadas

- **Python**
- **Airflow**
- **Pyspark**
- **AWS**

### DAGs
- Aqui estão os arquivos de orquestração do projeto.

![](utils/images/Captura%20de%20tela%20de%202024-08-03%2011-05-00.png)
![](utils/images/Captura%20de%20tela%20de%202024-08-03%2011-05-15.png)

### data
- Aqui se encontram os dados usados para no projeto.

### src
- Aqui se encontram os scripts para processamento, tanformações e carregamento dos dados.

### utils
- Aqui se encontram os arquivos usados no projeto como Configuração do Spark, jars para conexão com AWS, Classes e metodos usados ao longo do mesmo.
