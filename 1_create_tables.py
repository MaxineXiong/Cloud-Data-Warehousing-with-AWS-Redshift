# Import relevant modules
import configparser
import psycopg2
import sql_queries


def drop_tables(cur):
    """ Drop existing tables in the database using the queries from
    sql_queries.py.
    
    Args:
        - cur: A psycopg2.cursor object used to execute SQL commands.
    """
    # Iterate over a list of DROP TABLE queries
    for query in sql_queries.DROP_TABLE_QUERIES:
        # Execute each DROP TABLE query
        cur.execute(query)


def create_tables(cur):
    """ Create tables in the database using the queries from
    sql_queries.py.
    
    Args:
        - cur: A psycopg2.cursor object used to execute SQL commands.
    """
    # Iterate over a list of table names and corresponding CREATE
    # TABLE queries
    for table, query in zip(
        sql_queries.TABLES, sql_queries.CREATE_TABLE_QUERIES
    ):
        # Execute each CREATE TABLE query
        cur.execute(query)
        # Print a confirmation message after each table is created
        print(
            f'The {table} table has successfully been created!'
        )


def main():
    """ Main function to set up database connection, drop existing tables,
    and create new tables using the defined functions.
    """
    # Import the configparser module to read configuration files
    config = configparser.ConfigParser()
    # Read the configuration file containing AWS and Redshift settings
    config.read_file(open('dwh.cfg'))

    # Connect to the PostgreSQL database using settings from the config file
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER', 'HOST'),
            config.get('CLUSTER', 'DB_NAME'),
            config.get('CLUSTER', 'DB_USER'),
            config.get('CLUSTER', 'DB_PASSWORD'),
            config.get('CLUSTER', 'DB_PORT')
        )
    )
    # Set the session to autocommit mode
    conn.set_session(autocommit=True)
    # Create a cursor object for executing SQL commands
    cur = conn.cursor()

    # Drop existing tables
    drop_tables(cur)
    # Create new tables
    create_tables(cur)

    # Close the database connection
    conn.close()

    
# Execute the main function if this script is run directly
if __name__ == "__main__":
    main()
