import configparser, os, logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, split, trim, upper
from pyspark.sql.types import DateType, StructType, StructField, StringType, DoubleType, IntegerType

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('CONFIG.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
DESTINATION_S3_BUCKET = config['S3']['DESTINATION_S3_BUCKET']
IMMIGRATION_FILE_PATH = config['DATA']['IMMIGRATION_FILE_PATH']
LABELS_DESCRIPTION_FILE_PATH = config['DATA']['LABELS_DESCRIPTION_FILE_PATH']
DEMOGRAPHY_FILE_PATH = config['DATA']['DEMOGRAPHY_FILE_PATH']


# functions for data manipulations
def rename_columns(table, new_columns):
    '''
    Renames columns in a spark dataframe
    :param table: dataframe with old column names
    :param new_columns: list with new column names
    :return: dataframe with renamed column names
    '''
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def load_descriptions_labels(label_file_path, selected_label):
    '''
    Loads specified data labels from provided csv file
    :param label_file_path: data labels file path
    :param selected_label: label type to extract
    :return: id name pairs
    '''
    with open(label_file_path) as labels_file:
        labels_data = labels_file.read()

        selected_label_data = labels_data[labels_data.index(selected_label):]
        selected_label_data = selected_label_data[:selected_label_data.index(';')]

        lines = selected_label_data.split('\n')
        id_name_pairs = list()
        for line in lines:
            parts = line.split('=')
            if len(parts) != 2:
                continue
            id = parts[0].strip().strip("'")
            value = parts[1].strip().strip("'")
            id_name_pairs.append((id, value,))

        return id_name_pairs


# ETL for immigration data
def process_immigration_data(spark, input):
    '''
    Loads and processes us immigration data
    :param spark: spark session
    :param input: file path for the parquet files
    :return: immigration dataframe
    '''
    # load data
    df_spark = spark.read.parquet(input)

    # select columns for the fact table
    immigration_fact = df_spark.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',
                                       'arrdate', 'depdate', 'i94mode', 'i94visa', 'visatype',
                                       'i94cit', 'i94res', 'biryear', 'gender', 'insnum',
                                       'airline', 'admnum', 'fltno').distinct()

    # change date format
    format_SAS_to_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)

    immigration_fact = immigration_fact \
        .withColumn("arrdate", format_SAS_to_date(col("arrdate")).cast(DateType())) \
        .withColumn("depdate", format_SAS_to_date(col("depdate")).cast(DateType()))

    # rename columns
    new_column_names = ['citizen_id', 'year', 'month', 'airport_code', 'state_id', 'arrival_date',
                        'departure_date', 'travel_mode_id', 'visa_category_id', 'visa_type',
                        'citizen_country_id', 'residence_country_id', 'birth_year', 'gender',
                        'ins_num', 'airline', 'admin_num', 'flight_number']
    immigration_fact = rename_columns(immigration_fact, new_column_names)

    return immigration_fact


# ETL for demographic data
def process_demography_data(spark, input):
    '''
    Loads and processes us cities demographic data
    :param spark: spark session
    :param input: file path for the csv file
    :return: demography dataframe
    '''
    # load data
    schema = StructType([
        StructField('city', StringType()),
        StructField('state', StringType()),
        StructField('median_age', DoubleType()),
        StructField('male_population', IntegerType()),
        StructField('female_population', IntegerType()),
        StructField('total_population', IntegerType()),
        StructField('number_of_veterans', IntegerType()),
        StructField('foreign-born', IntegerType()),
        StructField('average_household_size', DoubleType()),
        StructField('state_id', StringType()),
        StructField('race', StringType())
    ])
    demog_df = spark.read.csv(input, sep=';', header=True, schema=schema)
    demog_df = demog_df.dropDuplicates(['city', 'state'])
    demog_df = demog_df.drop(col('state'))
    demog_df = demog_df.withColumn('city', upper(col('city')))

    return demog_df


# ETL for labels descriptions
def process_travel_mode_data(spark, input):
    '''
    Loads and processes travel mode data.
    :param spark: spark session
    :param input: data labels file path
    :return: processed dataframe
    '''
    travel_mode_label_pairs = load_descriptions_labels(input, 'i94model')
    schema = ['travel_mode_id', 'travel_mode_name']
    travel_mode_df = spark.createDataFrame(data=travel_mode_label_pairs, schema=schema)
    travel_mode_df = travel_mode_df.withColumn('travel_mode_id', travel_mode_df['travel_mode_id'].cast(IntegerType()))
    return travel_mode_df


def process_country_data(spark, input):
    '''
    Loads and processes country data.
    :param spark: spark session
    :param input: data labels file path
    :return: processed dataframe
    '''
    country_label_pairs = load_descriptions_labels(input, 'i94cntyl')
    schema = ['country_id', 'country_name']
    country_df = spark.createDataFrame(data=country_label_pairs, schema=schema)
    country_df = country_df.withColumn('country_name',
                                       regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'NA'))
    country_df = country_df.withColumn('country_id', country_df['country_id'].cast(IntegerType()))
    return country_df


def process_visa_category_data(spark, input):
    '''
    Loads and processes visa category data.
    :param spark: spark session
    :param input: data labels file path
    :return: processed dataframe
    '''
    visa_category_label_pairs = load_descriptions_labels(input, 'I94VISA')
    schema = ['visa_category_id', 'visa_category_name']
    visa_category_df = spark.createDataFrame(data=visa_category_label_pairs, schema=schema)
    visa_category_df = visa_category_df.withColumn('visa_category_id',
                                                   visa_category_df['visa_category_id'].cast(IntegerType()))
    return visa_category_df


def process_us_states_data(spark, input):
    '''
    Loads and processes us states data.
    :param spark: spark session
    :param input: data labels file path
    :return: processed dataframe
    '''
    us_states_label_pairs = load_descriptions_labels(input, 'i94addrl')
    schema = ['us_state_id', 'us_state_name']
    us_states_df = spark.createDataFrame(data=us_states_label_pairs, schema=schema)
    us_states_df = us_states_df.where('us_state_id != "99"')
    return us_states_df


def process_airport_codes_data(spark, input):
    '''
    Loads and processes airport codes data.
    :param spark: spark session
    :param input: data labels file path
    :return: processed dataframe
    '''
    airport_codes_label_pairs = load_descriptions_labels(input, '$i94prtl')
    schema = ['airport_code', 'airport_code_name']
    airport_codes_df = spark.createDataFrame(data=airport_codes_label_pairs, schema=schema)
    split_column = split(airport_codes_df.airport_code_name, ',')
    airport_codes_df = airport_codes_df \
        .withColumn('city', split_column.getItem(0)) \
        .withColumn('us_state_id', trim(split_column.getItem(1))) \
        .drop(col('airport_code_name'))
    return airport_codes_df


# Data quality checks
def labels_unique_key_check(table_name, df):
    '''
    Checks if the first column in the table is unique.
    :param table_name: name of the table
    :param df: spark dataframe
    '''
    if df.select(df.columns[0]).distinct().count() != df.count():
        raise ValueError(f'Data quality check not passed for {table_name}. Key is not unique')


def demography_unique_key_check(table_name, df):
    '''
    Checks if the first column in the city demography table is unique.
    :param table_name: name of the table
    :param df: spark dataframe
    '''
    if df.select(['city', 'state_id']).distinct().count() != df.count():
        raise ValueError(f'Data quality check not passed for {table_name}. Key is not unique')


def empty_table_check(table_name, df):
    '''
    Checks if the table is empty
    :param table_name: name of the table
    :param df: spark dataframe
    '''
    if df.count() == 0:
        raise ValueError(f'Data quality check not passed for {table_name}. Table is empty')


def upload_dim_tables(table_name, df, output):
    df.write.mode('overwrite').parquet(output + f'{table_name}.parquet')


def main():
    # create spark session
    spark = SparkSession.builder. \
        config("spark.jars.repositories", "https://repos.spark-packages.org/"). \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11"). \
        config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"). \
        config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']). \
        config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']). \
        enableHiveSupport().getOrCreate()

    logging.info('Spark session created')

    # load and process data into ready-to-load dataframes
    immigration_fact = process_immigration_data(spark, IMMIGRATION_FILE_PATH)
    demog_df = process_demography_data(spark, DEMOGRAPHY_FILE_PATH)
    travel_mode_df = process_travel_mode_data(spark, LABELS_DESCRIPTION_FILE_PATH)
    country_df = process_country_data(spark, LABELS_DESCRIPTION_FILE_PATH)
    visa_category_df = process_visa_category_data(spark, LABELS_DESCRIPTION_FILE_PATH)
    us_states_df = process_us_states_data(spark, LABELS_DESCRIPTION_FILE_PATH)
    airport_codes_df = process_airport_codes_data(spark, LABELS_DESCRIPTION_FILE_PATH)

    logging.info('All tables loaded to dataframes')

    # run data quality checks
    label_tables = {
        'dim_travel_mode': travel_mode_df,
        'dim_country': country_df,
        'dim_visa_category': visa_category_df,
        'dim_us_category': us_states_df,
        'dim_airport_codes': airport_codes_df
    }
    for table_name, df in label_tables.items():
        labels_unique_key_check(table_name, df)
    demography_unique_key_check('dim_city_demography', demog_df)

    logging.info('Unique key checks passed')

    tables = {
        'immigration_facts': immigration_fact,
        'dim_city_demography': demog_df,
        'dim_travel_mode': travel_mode_df,
        'dim_country': country_df,
        'dim_visa_category': visa_category_df,
        'dim_us_states': us_states_df,
        'dim_airport_codes': airport_codes_df
    }
    for table_name, df in tables.items():
        empty_table_check(table_name, df)

    logging.info('Empty tables check passed')

    # upload immigration_facts
    logging.info('writing immigration table')
    immigration_fact.write.mode('overwrite').partitionBy('year', 'month', 'airport_code').parquet(
        DESTINATION_S3_BUCKET + 'immigration_facts.parquet')

    # upload dimension tables
    dimension_tables = {
        'dim_city_demography': demog_df,
        'dim_travel_mode': travel_mode_df,
        'dim_country': country_df,
        'dim_visa_category': visa_category_df,
        'dim_us_category': us_states_df,
        'dim_airport_codes': airport_codes_df
    }
    for table_name, df in dimension_tables.items():
        logging.info(f'writing {table_name}')
        upload_dim_tables(table_name, df, output=DESTINATION_S3_BUCKET)


if __name__ == '__main__':
    main()
