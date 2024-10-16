import mysql.connector as mysql
import os
from dotenv import load_dotenv

class DbConnector:
    """
    Connects to the MySQL server using environment variables for configuration.
    Requires the following environment variables:
    - DB_HOST
    - DB_DATABASE
    - DB_USER
    - DB_PASSWORD
    """

    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Retrieve database connection parameters from environment variables
        HOST = os.getenv('DB_HOST')
        DATABASE = os.getenv('DB_DATABASE')
        USER = os.getenv('DB_USER')
        PASSWORD = os.getenv('DB_PASSWORD')
        PORT = os.getenv('DB_PORT', 3306)  # Default to 3306 if not set

        # Connect to the database
        try:
            self.db_connection = mysql.connect(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                port=PORT
            )
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
