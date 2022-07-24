import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops each table using the queries in `drop_table_queries` list.
    
    Args:
        cur: database cursor.
        conn: database connection.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates staging, facts and dimension tables using the queries in `create_table_queries` list.
    
    Args:
        cur: database cursor.
        conn: database connection.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Reads configurations from capstone.cfg file
    
    - Establishes connection with the sparkify database on redshift instance and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
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
    print("connection establisted")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()