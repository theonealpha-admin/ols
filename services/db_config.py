import psycopg2
from services.loger import logger

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME_2 = "trading_system"
DB_USER = "postgres"
DB_PASS = "onealpha12345"
DB_SCHEMA = "public"


def create_database_if_not_exists(database_name):
    # Connect to the default database (usually "postgres")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname="postgres", 
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = True  # required for creating databases
    cur = conn.cursor()

    # Check if the database exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
    exists = cur.fetchone()

    if not exists:
        # If the database does not exist, create it
        create_query = f"CREATE DATABASE {database_name};"
        cur.execute(create_query)
        print(f"Database '{database_name}' created successfully.")
    else:
        pass
        # print(f"Database '{database_name}' already exists. Skipping creation.")

    cur.close()
    conn.close()


def database_Creation():
    # Check and create databases if they don't exist
    create_database_if_not_exists(DB_NAME_2)


def get_db_connection():
    """Establish a connection to the PostgreSQL database and return the connection object."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME_2,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to the PostgreSQL database: {e}")
        raise e

# if __name__ == "__main__":
#     database_Creation()