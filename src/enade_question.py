import pyspark.sql.functions as f
import pyspark.sql.types as t

FILE_PATHS = {
    'df_aluno': 'data/enade2019/parquet/aluno/',
    'df_curso': 'data/enade2019/parquet/curso/'
}


class EnadeQuestion:
    def __init__(self, spark_session):

        self.spark = spark_session
        #self.read_options = {'header': True, 'sep': '|'}

    def read_data(self):
        self.df_aluno_parquet = (self.spark.read.format('parquet').load(
            FILE_PATHS['df_aluno']))

        self.df_curso_parquet = (self.spark.read.format('parquet').load(
            FILE_PATHS['df_curso']))

    def print_question1(self):
        print(
            "*********************************************************************************************************************************************************\n"
            "1 - Quantos alunos NAO quiseram declarar a cor/raca em 2019 (Entenda que o aluno marcou a opcaoo de 'nao quero declarar' nessa pergunta)?\n"
            "R: ",
            self.df_aluno_parquet.select('*').filter(
                f.col('TP_COR_RACA') == 0).count())

    def print_question2(self):
        print(
            "\n2 - Qual  o numero de alunos do Sexo Feminino que nasceram no estado de codigo 35\n"
            "R: ",
            self.df_aluno_parquet.select(
                'CO_UF_NASCIMENTO',
                'TP_SEXO').filter((f.col('TP_SEXO') == 1)
                                  & (f.col('CO_UF_NASCIMENTO') == 35)).count())

    def print_question3(self):
        print(
            "\n3-  Quais os codigos dos cinco primeiros estado brasileiro em que nasceram mais alunos matriculados em cursos de ensino superior em 2019 e a quantidade desse alunos\n"
            "R: ", (self.df_aluno_parquet.select('CO_UF_NASCIMENTO').groupBy(
                'CO_UF_NASCIMENTO').count().orderBy(
                    f.col('count').desc()).limit(5).toPandas()))

    def print_question4(self):
        print(
            "\n4- Qual o nome dos dois cursos que contem a maior quantidade total de inscritos?\n"
            "R: ", (self.df_curso_parquet.join(
                self.df_aluno_parquet, 'CO_CURSO').select(
                    'NO_CURSO').groupBy('NO_CURSO').count().orderBy(
                        f.col('count').desc()).limit(2).toPandas()))

    def run(self):
        self.read_data()
        self.print_question1()
        self.print_question2()
        self.print_question3()
        self.print_question4()
