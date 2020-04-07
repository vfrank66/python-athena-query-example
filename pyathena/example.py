"""Module explaining how to use PyAthena in conjuction with Pandas."""

from pyathena import connect
from pyathena.pandas_cursor import PandasCursor
from os import environ

from helpers import timeit

@timeit(msg='PyAthena Query: ')
def read_athena(cursor, schema, table):
    """ """
    return cursor.execute(
        f'SELECT * FROM "{schema}".{table} limit 1000;').as_pandas()


if __name__ == '__main__':
    src_db = '<insert source db>'
    environment = 'dev'
    region_name = 'us-east-1'
    aws_access_key_id = environ.get('aws_access_key_id_athena')
    aws_secret_access_key = environ.get('aws_secret_access_key_athena')
    s3_staging_dir = f's3://<insert athena staging dir>'
    schema = f'<insert source glue catalog schema>' # f'{environment}-{src_db}'

    cursor = connect(s3_staging_dir=s3_staging_dir,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name,
                    cursor_class=PandasCursor).cursor()

    df = read_athena(cursor, schema)
    
    print(df.describe())
    print(df.head())
