import mysql.connector
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Establishes a database connection and returns the connection and cursor references.

    :return: Returns (cur, conn), a cursor and connection reference.
    """
    # Connect to MySQL server
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="40363933",
    )
    cur = conn.cursor()

    # Create sparkify database if not exists
    cur.execute("CREATE DATABASE IF NOT EXISTS sparkifydb")

    # Switch to sparkify database
    cur.execute("USE sparkifydb")

    return cur, conn


def drop_tables(cur, conn):
    """
    Executes all the drop table queries defined in sql_queries.py.

    :param cur: Cursor to the database.
    :param conn: Database connection reference.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes all the create table queries defined in sql_queries.py.

    :param cur: Cursor to the database.
    :param conn: Database connection reference.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Driver main function.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    print("Tables dropped successfully!!")

    create_tables(cur, conn)
    print("Tables created successfully!!")

    conn.close()


if __name__ == "__main__":
    main()