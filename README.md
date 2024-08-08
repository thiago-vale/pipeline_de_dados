# Pipeline de Dados

https://github.com/thiago-vale/pipeline_de_dados

## Sobre o Projeto

Este projeto consiste no desenvolvimento de pipelines de dados para a construção de um Delta Lake.

## Estrutura das pastas
```
.
├── dags - Arquivos para orquestração
├── data - Dados usados no projeto
│   └── raw
├── notebooks - Nootebooks para analise
├── src - Pastas e Arquivos .py para realizar o ETL
│   ├── bronze_to_silver
│   ├── landing_to_bronze
│   ├── silver_to_gold
│   └── source_to_landing
└── utils - Classes e Objeto usados no Projeto
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

### src
https://github.com/thiago-vale/pipeline_de_dados/tree/master/src

- Aqui se encontram os scripts para processamento, tanformações e carregamento dos dados, os scripts estão separados de acordo com suas camadas do delta lake.

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-30-45.png)

### source_to_landing

- Script de ingestão da Camada Landing

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-31-18.png)

### landing_to_bronze

- Script de ingestão da Camada Bronze

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-31-28.png)

### bronze_to_silver

- Script de ingestão da Camada Silver

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-31-44.png)

### silver_to_gold

- Script de ingestão da Camada Gold

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-32-41.png)

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-32-57.png)

![](utils/images/Captura%20de%20tela%20de%202024-08-08%2009-33-05.png)

### utils
https://github.com/thiago-vale/pipeline_de_dados/tree/master/utils

- Aqui se encontram os arquivos usados no projeto como Configuração do Spark, jars para conexões, Classes e metodos usados ao longo do mesmo.

#### credentials.py

- Classe e metodos paa leitura das credenciais do projeto.

#### etl.py

- Classe Base para realização do ETL.

#### extract.py

- Classe contendo todos metodos para extração de dados usados no projeto.

#### load.py

- Classe contendo todos metodos para grvação de dados usados no projeto.

#### spark_config.py

- Classe que estabelece toda a configuração do spark utilizada no projeto

#### transform.py

- Classe contendo todos metodos para tranformação de dados usados no projeto.

#### spark_jars
https://github.com/thiago-vale/pipeline_de_dados/tree/master/utils/spark_jars

- Aqui estão .jars para o spark estabelecer conexões.

PS: Este projeto por hora está sendo rodado localmente