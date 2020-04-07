"""Module explaining how to use AWS Data Wrangler in conjuction with Pandas."""
from os import environ

import awswrangler as wr

from helpers import timeit

@timeit(msg='AWS Data Wrangler: ')
def read_athena(sql, wr_session, schema, s3_staging_dir):
    """ Read Athena tables using AWS Wrangler"""

    return wr_session.pandas.read_sql_athena(
        sql=sql,
        database=schema,
        s3_output=s3_staging_dir,
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

if __name__ == '__main__':
    src_db = '<insert source db>'
    environment = 'dev'
    region_name = 'us-east-1'
    s3_staging_dir = f's3://<insert athena staging dir>'
    schema = f'<insert source glue catalog schema>' # f'{environment}-{src_db}'
    table_name = f'<insert table name>'

    # PyAthena uses secrets, data wrangler can use secrets or aws profile
    aws_access_key_id = None # environ.get('aws_access_key_id_athena')
    aws_secret_access_key = None # environ.get('aws_secret_access_key_athena')
    credentials = {
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key
    }
    profile = 'default'

    df = read_athena(
        f'SELECT * FROM "{schema}".{table_name} limit 1000;',
        wr_session=get_wr_session(region=region_name, profile=profile, credentials=credentials),
        schema=schema,
        s3_staging_dir=s3_staging_dir)

    print(df.describe())
    print(df.head())
