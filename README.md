# Spark_Enade2019
Jobs Spark

# DataSet
- Baixe os dados em: https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip
- Após a extração os dados, certifique-se que colocou os arquivos SUP_ALUNO_2019.CSV e SUP_CURSO_2019.CSV na pasta data do projeto

# Comando para o submit
spark-submit --py-files  src/enade_to_parquet.py,src/enade_question.py src/spark_app.py
