from pyspark.sql import types

DATA_DIR = "/opt/airflow/data"
JAR_DIR = "/opt/airflow/jars"
HOME_DIR = "/opt/airflow"
BUCKET_NAME = 'de-boootcamp-root'

dates = (
    '2022-10-31',
    '2022-11-01',
    '2022-11-02',
    '2022-11-03',
    '2022-11-04',
    '2022-11-05',
    '2022-11-06',
    '2022-11-07',
    '2022-11-08',
    '2022-11-09',
    '2022-11-10',
    '2022-11-11',
    '2022-11-12',
    '2022-11-13',
    '2022-11-14',
    '2022-11-15',
    '2022-11-16',
    '2022-11-17',
    '2022-11-18',
    '2022-11-19',
    '2022-11-20',
    '2022-11-21',
    '2022-11-22',
    '2022-11-23',
    '2022-11-24',
    '2022-11-25',
    '2022-11-26',
)

br_states = (
    'AC',
    'AL',
    'AP',
    'AM',
    'BA',
    'CE',
    'DF',
    'ES',
    'GO',
    'MA',
    'MT',
    'MS',
    'MG',
    'PA',
    'PB',
    'PR',
    'PE',
    'PI',
    'RJ',
    'RN',
    'RS',
    'RO',
    'RR',
    'SC',
    'SP',
    'SE',
    'TO',
)

execution_dict = dict(
    zip(
        dates,
        br_states
    )
)


COLS = [ 'TS_GERACAO', 'NR_TURNO', 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_ZONA',
       'NR_SECAO', 'NR_LOCAL_VOTACAO', 'CD_CARGO_PERGUNTA',
       'DS_CARGO_PERGUNTA', 'NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO',
       'DT_BU_RECEBIDO', 'QT_APTOS', 'QT_COMPARECIMENTO', 'QT_ABSTENCOES',
       'CD_TIPO_URNA', 'DS_TIPO_URNA', 'CD_TIPO_VOTAVEL', 'DS_TIPO_VOTAVEL',
       'NR_VOTAVEL', 'NM_VOTAVEL', 'QT_VOTOS',]

SCHEMA = types.StructType(
    [
        types.StructField('HH_GERACAO', types.StringType(), True),
        types.StructField('DT_GERACAO', types.StringType(), True),
        types.StructField('ANO_ELEICAO', types.IntegerType(), True),
        types.StructField('CD_TIPO_ELEICAO', types.IntegerType(), True),
        types.StructField('NM_TIPO_ELEICAO', types.StringType(), True),
        types.StructField('CD_PLEITO', types.IntegerType(), True),
        types.StructField('DT_PLEITO', types.StringType(), True),
        types.StructField('NR_TURNO', types.IntegerType(), True),
        types.StructField('CD_ELEICAO', types.IntegerType(), True),
        types.StructField('DS_ELEICAO', types.StringType(), True),
        types.StructField('SG_UF', types.StringType(), True),
        types.StructField('CD_MUNICIPIO', types.IntegerType(), True),
        types.StructField('NM_MUNICIPIO', types.StringType(), True),
        types.StructField('NR_ZONA', types.IntegerType(), True),
        types.StructField('NR_SECAO', types.IntegerType(), True),
        types.StructField('NR_LOCAL_VOTACAO', types.IntegerType(), True),
        types.StructField('CD_CARGO_PERGUNTA', types.IntegerType(), True),
        types.StructField('DS_CARGO_PERGUNTA', types.StringType(), True),
        types.StructField('NR_PARTIDO', types.IntegerType(), True),
        types.StructField('SG_PARTIDO', types.StringType(), True),
        types.StructField('NM_PARTIDO', types.StringType(), True),
        types.StructField('DT_BU_RECEBIDO', types.StringType(), True),
        types.StructField('QT_APTOS', types.LongType(), True),
        types.StructField('QT_COMPARECIMENTO', types.LongType(), True),
        types.StructField('QT_ABSTENCOES', types.LongType(), True),
        types.StructField('CD_TIPO_URNA', types.LongType(), True),
        types.StructField('DS_TIPO_URNA', types.StringType(), True),
        types.StructField('CD_TIPO_VOTAVEL', types.LongType(), True),
        types.StructField('DS_TIPO_VOTAVEL', types.StringType(), True),
        types.StructField('NR_VOTAVEL', types.LongType(), True),
        types.StructField('NM_VOTAVEL', types.StringType(), True),
        types.StructField('QT_VOTOS', types.LongType(), True),
        types.StructField('NR_URNA_EFETIVADA', types.LongType(), True),
        types.StructField('CD_CARGA_1_URNA_EFETIVADA', types.StringType(), True),
        types.StructField('CD_CARGA_2_URNA_EFETIVADA', types.DoubleType(), True),
        types.StructField('CD_FLASHCARD_URNA_EFETIVADA', types.StringType(), True),
        types.StructField('DT_CARGA_URNA_EFETIVADA', types.StringType(), True),
        types.StructField('DS_CARGO_PERGUNTA_SECAO', types.StringType(), True),
        types.StructField('DS_AGREGADAS', types.StringType(), True),
        types.StructField('DT_ABERTURA', types.StringType(), True),
        types.StructField('DT_ENCERRAMENTO', types.StringType(), True),
        types.StructField('QT_ELEITORES_BIOMETRIA_NH', types.LongType(), True),
        types.StructField('DT_EMISSAO_BU', types.TimestampType(), True),
        types.StructField('NR_JUNTA_APURADORA', types.LongType(), True),
        types.StructField('NR_TURMA_APURADORA', types.LongType(), True)
    ]
)
