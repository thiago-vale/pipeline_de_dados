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
https://github.com/thiago-vale/pipeline_de_dados/blob/master/dags/spark_dag.py
- Aqui estão os arquivos de orquestração do projeto.

![](utils/images/Captura%20de%20tela%20de%202024-08-03%2011-05-00.png)
![](utils/images/Captura%20de%20tela%20de%202024-08-03%2011-05-15.png)

### data
https://github.com/thiago-vale/pipeline_de_dados/tree/master/data/raw

- Aqui se encontram os dados usados para no projeto.

### src
https://github.com/thiago-vale/pipeline_de_dados/tree/master/src

- Aqui se encontram os scripts para processamento, tanformações e carregamento dos dados, os scripts estão separados de acordo com suas camadas do data lake.

### utils
https://github.com/thiago-vale/pipeline_de_dados/tree/master/utils

- Aqui se encontram os arquivos usados no projeto como Configuração do Spark, jars para conexão com AWS, Classes e metodos usados ao longo do mesmo.

#### read.py
- Classe para leitura de arquivos e credenciais, pode se usar para ler variaveis de ambiente, arquivos ou credenciais em cofres.

#### spark_config.py

- Classe que estabelece com toda a configuração do spark já setada para que não seja preciso configurar o spark toda vez que se abra uma nova sessão.

#### write.py
- Classe para facilitar a escrita dos dados em stogaes e data warehouses.

#### spark_jars
https://github.com/thiago-vale/pipeline_de_dados/tree/master/utils/spark_jars

- Aqui estão os arquivos para que o spark estabeleça uma conexão com a aws.