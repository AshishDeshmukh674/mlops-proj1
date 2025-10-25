import os
import sys
import pymongo
import certifi
from urllib.parse import quote_plus, urlparse, urlunparse

from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME, MONGODB_URL_KEY

# Load the certificate authority file to avoid timeout errors when connecting to MongoDB
ca = certifi.where()


def escape_mongodb_url(mongodb_url: str) -> str:
    """
    Escapes special characters in the username and password of a MongoDB connection URL.
    
    Parameters:
    ----------
    mongodb_url : str
        The MongoDB connection URL that may contain unescaped special characters.
    
    Returns:
    -------
    str
        The MongoDB URL with properly escaped username and password.
    """
    try:
        # Check if URL contains credentials (username:password@)
        if "@" in mongodb_url and "://" in mongodb_url:
            # Split by protocol
            protocol, rest = mongodb_url.split("://", 1)
            
            # Check if credentials exist (there should be at least one @ for the host separator)
            if "@" in rest:
                # Find the position of the last @ which separates credentials from host
                # MongoDB URLs format: mongodb+srv://username:password@host
                last_at_index = rest.rfind("@")
                credentials = rest[:last_at_index]
                host_part = rest[last_at_index + 1:]
                
                # Split username and password (only split on first :)
                if ":" in credentials:
                    username, password = credentials.split(":", 1)
                    # Escape username and password
                    escaped_username = quote_plus(username)
                    escaped_password = quote_plus(password)
                    # Reconstruct the URL
                    return f"{protocol}://{escaped_username}:{escaped_password}@{host_part}"
        
        # If no credentials or already escaped, return as is
        return mongodb_url
    except Exception:
        # If any parsing fails, return original URL
        return mongodb_url


class MongoDBClient:
    """
    MongoDBClient is responsible for establishing a connection to the MongoDB database.

    Attributes:
    ----------
    client : MongoClient
        A shared MongoClient instance for the class.
    database : Database
        The specific database instance that MongoDBClient connects to.

    Methods:
    -------
    __init__(database_name: str) -> None
        Initializes the MongoDB connection using the given database name.
    """

    client = None  # Shared MongoClient instance across all MongoDBClient instances

    def __init__(self, database_name: str = DATABASE_NAME) -> None:
        """
        Initializes a connection to the MongoDB database. If no existing connection is found, it establishes a new one.

        Parameters:
        ----------
        database_name : str, optional
            Name of the MongoDB database to connect to. Default is set by DATABASE_NAME constant.

        Raises:
        ------
        MyException
            If there is an issue connecting to MongoDB or if the environment variable for the MongoDB URL is not set.
        """
        try:
            # Check if a MongoDB client connection has already been established; if not, create a new one
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)  # Retrieve MongoDB URL from environment variables
                if mongo_db_url is None:
                    raise Exception(f"Environment variable '{MONGODB_URL_KEY}' is not set.")
                
                # Escape special characters in the MongoDB URL
                mongo_db_url = escape_mongodb_url(mongo_db_url)
                
                # Establish a new MongoDB client connection
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
                
            # Use the shared MongoClient for this instance
            self.client = MongoDBClient.client
            self.database = self.client[database_name]  # Connect to the specified database
            self.database_name = database_name
            logging.info("MongoDB connection successful.")
            
        except Exception as e:
            # Raise a custom exception with traceback details if connection fails
            raise MyException(e, sys)