from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('SparkETLestab') \
    .getOrCreate()
path = 'gs://strongr_bucket/Estabelecimentos'
#path_save = '/media/vinicius/Storage/Stronger/Repositorio_Dados_Brutros/Estabelecimento/'





schema = StructType([
    StructField('CNPJ_BASICO',StringType()),
    StructField('CNPJ_ORDEM',StringType()),
    StructField('CNPJ_DV',StringType()),
    StructField('MATRIZ_FILIAL',StringType()),
    StructField('NOME_FANTASIA',StringType()),
    StructField('SITUACAO_CADASTRAL',StringType()),
    StructField('DATA_SITUACAO_CADASTRAL',StringType()),
    StructField('MOTIVO_SITUACAO',StringType()),
    StructField('NOME_CIDADE_EXT',StringType()),
    StructField('PAIS',StringType()),
    StructField('Data_Inicio_Atividade',StringType()),
    StructField('CNAE_PRINCIPAL',StringType()),
    StructField('CNAE_SECUNDARIA',StringType()),
    StructField('TIPO_LOGRADDOURO',StringType()),
    StructField('LOGRADOURO',StringType()),
    StructField('NUM',StringType()),
    StructField('COMPLEMENTO',StringType()),
    StructField('BAIRRO',StringType()),
    StructField('CEP',StringType()),
    StructField('UF',StringType()),
    StructField('MUNICIPIO',StringType()),
    StructField('DDD1',StringType()),
    StructField('TEL1',StringType()),
    StructField('DDD2',StringType()),
    StructField('TEL2',StringType()),
    StructField('DDD_FAX',StringType()),
    StructField('TEL_FAX',StringType()),
    StructField('E-MAIL',StringType()),
    StructField('SITUACAO_ESPECIAL',StringType()),

    StructField('DATA_SIT_ESPECIAL',StringType())])



df = spark.read.csv(path,
                   sep=';',
                   schema=schema,
                   header=False)

df = df.select(concat(df.CNPJ_BASICO,df.CNPJ_ORDEM, df.CNPJ_DV).alias('CNPJ_COMPLETO'), '*')


# Saving the data to BigQuery
df.write.format('bigquery') \
  .option('table', 'dw_octopus.estabelecimentos') \
  .save()