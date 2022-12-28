import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loop through the queries in the copy_table_queries list from sql_queries.py, execute and commit the queries
        These queries will copy data directly from the S3 buckets provided into Redshift staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Loop through the queries in the insert_table_queries list from sql_queries.py, execute and commit the queries
        These queries will transform the data loaded from S3 in the previous step and load them into the tables for our final data model
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Instantiate configparser instance, use to read in Redshift credentials from dwh.cfg file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    """ Use psycopg2 package to connect to Redshift database using cluster credentials provided
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    """ Load data from S3 into staging tables, then insert data from staging tables into final data model
    """
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    """ Close connection
    """
    conn.close()


if __name__ == "__main__":
    main()
