import pyspark.sql.functions as f
import pyspark.sql.types as t

FILE_PATHS = {
    'df_aluno': 'data/enade2019/SUP_ALUNO_2019.CSV',
    'df_curso': 'data/enade2019/SUP_CURSO_2019.CSV',
}


class Enade2019ToParquet:
    def __init__(self, spark_session):

        self.spark = spark_session
        self.read_options = {'header': True, 'sep': '|'}

    def read_data(self):
        self.df_aluno = (self.spark.read.format('csv').options(
            **self.read_options).load(FILE_PATHS['df_aluno']))

        self.df_curso = (self.spark.read.format('csv').options(
            **self.read_options).load(FILE_PATHS['df_curso']))

    def write_data(self):
        self.df_aluno.write.format('parquet').mode('overwrite').save(
            'data/enade2019/parquet/aluno/')

        self.df_curso.write.format('parquet').mode('overwrite').save(
            'data/enade2019/parquet/curso/')

    def run(self):
        self.read_data()
        self.write_data()
