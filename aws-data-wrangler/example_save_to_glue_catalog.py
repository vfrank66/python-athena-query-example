"""Module explaining how to use aws data wrangler in conjuction to 
run a query in athena into a dataframe and then output the dataframe into 
s3 and register the table with glue catalog!

Save to athena has been copied out becasue you should not be creating 
databases/tables unless you have read the code first.
"""
import boto3
import re 

from os import environ

import awswrangler as wr

class WranglerSettings:
    def __init__(self, schema, s3_staging_dir, glue_client, s3_client, s3_path, database_name, table):
        self.schema = schema
        self.s3_staging_dir = s3_staging_dir
        self.glue_client = glue_client
        self.s3_client = s3_client
        self.s3_path = s3_path
        self.database_name = database_name
        self.table = table

def read_athena(sql, wr_session, wr_settings):
    """ Read Athena tables using AWS Wrangler"""

    return wr_session.pandas.read_sql_athena(
        sql=sql,
        database=wr_settings.schema,
        s3_output=wr_settings.s3_staging_dir,
        ctas_approach=True,
    )


def get_wr_session(region, profile, credentials):
    """
    Create an AWS Wrangler session
    extra_args = {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "YOUR_KMY_KEY_ARN"
    }
    """

    if 'aws_access_key_id' in credentials and credentials['aws_access_key_id'] is not None:
        wr_session = wr.Session(region_name=region, athena_ctas_approach=True)
    else:
        wr_session = wr.Session(profile_name=profile,
                                region_name=region,
                                athena_ctas_approach=True)

    return wr_session


def default_null_and_unknown_columns_to_string(df):
    """ Find null columns with an unknown data type
    and convert it to a string column because Glue
    needs to have a schema.
    """
    # List the columns of the dataframe
    object_columns = list(df.select_dtypes(include=['object']).columns)

    # find all column which has all null values and no datatype and convert it to a string for glue
    null_columns = list(df.columns[df.isnull().any()])
    null_and_unknown_dtype_col = [value for value in object_columns if value in null_columns]
    if len(null_and_unknown_dtype_col) > 0:
        df[null_and_unknown_dtype_col] = df[null_and_unknown_dtype_col].astype(str)

    # Find columns with all NAs and convert them to string
    na_col = [i for i in df.columns if df[i].isnull().all()]
    if len(na_col) > 0:
        for column in df:
            df[column] = df[column].astype(str)

    return df


def create_database_if_not_exists(glue_client, database_name):
    """ Checks to see if the database exists and
    creates one if it does not exist.
    """
    try:
        glue_client.get_database(Name=database_name)
    except Exception as ex:
        if ex.response['Error']['Code'] == 'EntityNotFoundException':
            print(f'Did not find Glue database {database_name}... creating database')
            glue_client.create_database(
                DatabaseInput={
                    'Name': f'{database_name}',
                    'Description': f'Auto-generated database from data wrangler examples',
                })


def _save_df_to_glue(wr_session, df, database_name, s3_path, table):
    return wr_session.pandas.to_csv(dataframe=df,
                                    path=f"{s3_path}",
                                    database=database_name,
                                    table=f'{table}',
                                    mode='overwrite',
                                    preserve_index=False)


def save_df_to_glue(wr_settings, df, table, s3_path, wr_session):
    """Saves a dataframe to athena data store
    and register the s3 location as a table in the glue catalog
    Do not change the preserve_index to True.  It will cause the Glue table registration to fail
    """

    create_database_if_not_exists(glue_client=wr_settings.glue_client, database_name=wr_settings.database_name)
    df = default_null_and_unknown_columns_to_string(df)
    _save_df_to_glue(wr_session, df, dest_db, s3_path, table)


def save_to_athena_datastore(df, key, s3_destination, wr_session, wr_settings):
    """Saving the data to the data materializer data store for athena crawl."""
    full = f"{s3_destination}/{key}"
    s3_bucket, remove_key = re.match(r"s3:\/\/(.+?)\/(.+)", full).groups()

    # Remove the partitions before proceeding
    wr_settings.s3_client.delete_object(Bucket=s3_bucket, Key=remove_key)

    # Save dataframe to Glue
    save_df_to_glue(wr_settings, df, key, full, wr_session)


def get_boto_client(region, credentials, client_type):
    """ Create a boto3 client session for the param: client_type"""
    if credentials.aws_access_key_id is None:
        boto_client = boto3.client(service_name=client_type, region_name=region)
    else:
        boto_client = boto3.client(service_name=client_type,
                                   region_name=region,
                                   aws_access_key_id=credentials.aws_access_key_id,
                                   aws_secret_access_key=credentials.aws_secret_access_key,
                                   aws_session_token=credentials.aws_session_token)
    return boto_client


if __name__ == '__main__':
    src_db = '<insert source db>'
    environment = 'dev'
    region_name = 'us-east-1'
    s3_staging_dir = f's3://<insert athena staging dir>'
    schema = f'<insert source glue catalog schema>' # f'{environment}-{src_db}'
    src_table_name = f'<insert table name>'
    
    # PyAthena uses secrets, data wrangler can use secrets or aws profile
    aws_access_key_id = None # environ.get('aws_access_key_id_athena')
    aws_secret_access_key = None # environ.get('aws_secret_access_key_athena')
    credentials = {
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key
    }
    profile = 'default'

    # '-' is a reserved letter in athena databases, remove
    dest_db = 'test_database_python_pandas_athena_example'.replace('-','_')
    dest_table_name = 'testing-stuff'.replace('-','_')
    dest_s3_path = s3_staging_dir
    dest_s3_file_name = f'testing'

    s3_client = get_boto_client(region_name, credentials, 's3')
    glue_client = get_boto_client(region_name, credentials, 'glue')


    wr_settings = WranglerSettings(schema, s3_staging_dir, glue_client, s3_client, dest_s3_path, dest_db, table=dest_table_name)
    wr_session = get_wr_session(region=region_name, profile=profile, credentials=credentials)

    df = read_athena(
        f'SELECT * FROM "{wr_settings.schema}".{src_table_name} limit 1000;',
        wr_session=wr_session,
        wr_settings=wr_settings)

    print(df.describe())
    print(df.head())


    # save_to_athena_datastore(df, file_name, s3_path, wr_session, wr_settings)
