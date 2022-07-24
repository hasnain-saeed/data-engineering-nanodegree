import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads song and log files data from s3 storage to staging table using the queries in `copy_table_queries` list.
    
    Args:
        cur: database cursor.
        conn: database connection.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts the data from staging tables to facts and dimenstion tables using the queries in `insert_tables` list.
    
    Args:
        cur: database cursor.
        conn: database connection.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        
def run_quality_checks(cur, conn):
    '''
    Run the data quality checks on the tables using the queries in `data_quality_queries` list.
    
    Args:
        cur: database cursor.
        conn: database connection.
    '''
    for query in data_quality_queries:
        cur.execute(query)
        conn.commit()
        
    
def main():
    '''
    - Reads configurations from capstone.cfg file
    
    - Establishes connection with the sparkify database on redshift instance and gets
    cursor to it.  
    
    - Loads the data from s3 storage to staging tables
    
    - Loads the data from staging tables to facts and dimension tables
    
    - Finally, closes the connection. 
    '''
    config = configparser.ConfigParser()
    config.read('capstone.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get('CLUSTER','HOST'),
        config.get('CLUSTER','DB_NAME'),
        config.get('CLUSTER','DB_USER'),
        config.get('CLUSTER','DB_PASSWORD'),
        config.get('CLUSTER','DB_PORT')
    ))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_quality_checks(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()