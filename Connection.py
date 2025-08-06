# connection.py
import psycopg2
import psycopg2.pool
import logging
import threading
import Config  # Use the new config file

# Connection pool for better performance
_connection_pool = None
_pool_lock = threading.Lock()


def get_connection_pool():
    """
    Creates and returns a connection pool to the database.
    Uses a Singleton pattern for better performance.
    """
    global _connection_pool

    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:  # Double-check locking
                try:
                    _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=1,
                        maxconn=5,
                        host=Config.DB_HOST,
                        database=Config.DB_NAME,
                        user=Config.DB_USER,
                        password=Config.DB_PASS
                    )
                    logging.info("Connection pool created successfully")
                except Exception as e:
                    logging.error(f"Error creating connection pool: {e}")
                    raise

    return _connection_pool


def get_db_connection():
    """
    Gets a connection from the pool. Must be returned with return_db_connection!
    """
    try:
        pool = get_connection_pool()
        conn = pool.getconn()
        if conn:
            return conn
        else:
            logging.error("Could not get a connection from the pool")
            return None
    except Exception as e:
        logging.error(f"Error getting connection from pool: {e}")
        return None


def return_db_connection(conn):
    """
    Returns a connection to the pool for reuse.
    """
    try:
        if conn:
            pool = get_connection_pool()
            pool.putconn(conn)
    except Exception as e:
        logging.error(f"Error returning connection to pool: {e}")


def close_connection_pool():
    """
    Closes all connections in the pool. Call at the end of the program.
    """
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logging.info("Connection pool closed")