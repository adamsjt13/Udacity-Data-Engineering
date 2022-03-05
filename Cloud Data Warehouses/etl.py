import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """"
    Loads information from S3 bucket
    into staging_events and staging_songs
    tables. This is accomplished using
    AWS S3 COPY command. More detail
    can be found in sql_queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads information from staging tables
    into dimensional tables. This is accomplished
    via SQL to SQL ETL. More detail can be
    found in sql_queries.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads data into staging tables and also
    loads data from staging tables into
    dimensional tables.
    
    You will need the following information in your dwh.cfg file:
    
    [CLUSTER]
    HOST (*AWS_ENDPOINT)
    DB_NAME
    DB_USER
    DB_PASSWORD
    DB_PORT
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()