# Pipeline de Dados

https://github.com/thiago-vale/pipeline_de_dados

## Sobre o Projeto

Este projeto consiste no desenvolvimento de pipelines de dados para a construção de um Delta Lake utilizando uma estrutura medalion.

## Próximos passos

- Construção de um cluster kubernetes para subir a aplicação
- Provisionamento de infraestrutura usando terraform 

## Estrutura das pastas
```
.
├── dags
├── data
│   └── raw
├── logs
│   └── spark-events
├── metrics
│   ├── bronze_to_silver
│   ├── landing_to_bronze
│   ├── silver_to_gold
│   └── source_to_landing
├── notebboks
├── src
│   ├── bronze_to_silver
│   ├── landing_to_bronze
│   ├── silver_to_gold
│   └── source_to_landing
└── utils
    └── spark_jars
```

## Tecnologias Utilizadas

- **Python**
- **Airflow**
- **Pyspark**
- **AWS**

### DAGs
https://github.com/thiago-vale/pipeline_de_dados/blob/master/dags/spark_dag.py
- Aqui estão os arquivos de orquestração do projeto.

![](utils/images/Captura%20de%20tela%20de%202024-08-07%2017-27-56.png)
![](utils/images/Captura%20de%20tela%20de%202024-08-07%2017-27-49.png)

### data
https://github.com/thiago-vale/pipeline_de_dados/tree/master/data/raw

- Aqui se encontram os dados usados para no projeto.

### logs

https://github.com/thiago-vale/pipeline_de_dados/tree/master/logs

- Aqui se encontram os logs como eventos do spark e logs do pipeline para análise.

### metrics

https://github.com/thiago-vale/pipeline_de_dados/tree/master/metrics

aqui se encontram as métricas dos pipelines coletadas com Spark Measure

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
