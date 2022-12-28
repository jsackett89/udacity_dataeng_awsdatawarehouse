import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Loop through queries in drop_table_queries list from sql_queries.py, execute and commit the queries
        These queries will drop all tables in the database so that new ones can be created without interference
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Loop through queries in the create_table_queries list from sql_queries.py, execute and commit the queries
        These queries create both the staging tables to store data loaded directly from S3 as well as the tables where data will be loaded
        in our final data model
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Main function that executes logic in the functions defined above.
        First, instantiate configparser instance, use to read in Redshift credentials from dwh.cfg file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    """ Use psycopg2 package to connect to Redshift database using cluster credentials provided
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    """ Drop tables (if they exist) and create them using the connections and cursors defined above
    """
    drop_tables(cur, conn)
    create_tables(cur, conn)

    """ Close the connection
    """
    conn.close()


if __name__ == "__main__":
    main()
