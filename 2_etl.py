# Import relevant modules
import configparser
import psycopg2
import sql_queries


def load_staging_tables(cur):
    """ Load data into staging tables from S3 source.
    
    Args:
        - cur: A psycopg2.cursor object used to execute SQL commands.
    """
    # Iterate over a list of staging table names and corresponding
    # COPY queries
    for table, query in zip(
        sql_queries.TABLES[:2], sql_queries.COPY_TABLE_QUERIES
    ):
        # Execute each COPY query to load data into each staging table
        cur.execute(query)
        # Print a confimration message after each table is loaded
        print(
            f'Data has successfully been loaded to the {table} table!'
        )


def insert_tables(cur):
    """ Insert data from staging tables into dimensional and fact tables.
    
    Args:
        - cur: A psycopg2.cursor object used to execute SQL commands.
    """
    # Iterate over a list of target dim and fact table names and corresponding
    # INSERT queries
    for table, query in zip(
        sql_queries.TABLES[2:], sql_queries.INSERT_TABLE_QUERIES
    ):
        # Execute each INSERT query to load data into the target tables
        cur.execute(query)
        # Print a confirmation message after each table is loaded
        print(
            f'Data has successfully been loaded to the {table} table!'
        )


def main():
    """ Main function to set up database connection, load data into the staging
    tables, and transform data into the dimensional and fact tables.
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

    # Load data into the staging tables in the Redshift cluster
    # through COPY statements
    load_staging_tables(cur)
    # Insert data into the target dimensional and fact tables using
    # both staging tables
    insert_tables(cur)

    # Close the database connection
    conn.close()


# Execute the main function if this script is run directly
if __name__ == "__main__":
    main()
