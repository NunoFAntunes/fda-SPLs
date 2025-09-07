"""
Database connection management for SPL document insertion.
Handles PostgreSQL connections, transactions, and configuration.
"""

import os
import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Dict, Any, Optional
import json

# Load environment variables from .env file
from . import env_loader

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration management."""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'fda_spl'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
        }
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return (
            f"host={self.config['host']} "
            f"port={self.config['port']} "
            f"dbname={self.config['database']} "
            f"user={self.config['user']} "
            f"password={self.config['password']}"
        )
    
    def validate(self) -> bool:
        """Validate configuration completeness."""
        required_fields = ['host', 'database', 'user']
        for field in required_fields:
            if not self.config.get(field):
                logger.error(f"Missing required database configuration: {field}")
                return False
        return True


class DatabaseConnection:
    """Database connection and transaction management."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.pool: Optional[ThreadedConnectionPool] = None
        
        if not self.config.validate():
            raise ValueError("Invalid database configuration")
    
    def initialize_pool(self, minconn: int = 1, maxconn: int = 10):
        """Initialize connection pool."""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dsn=self.config.get_connection_string(),
                cursor_factory=RealDictCursor
            )
            logger.info(f"Database connection pool initialized: {minconn}-{maxconn} connections")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {str(e)}")
            raise
    
    def close_pool(self):
        """Close connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with automatic cleanup."""
        if not self.pool:
            self.initialize_pool()
        
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    @contextmanager
    def transaction(self):
        """Transaction context manager with automatic rollback on error."""
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                yield cursor
                conn.commit()
                logger.debug("Transaction committed successfully")
            except Exception as e:
                conn.rollback()
                logger.error(f"Transaction rolled back due to error: {str(e)}")
                raise
            finally:
                cursor.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()
                logger.info("Database connection test successful")
                return result['test'] == 1  # Use dict key instead of index
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            print(f"DEBUG: Database connection test failed: {str(e)}")  # Debug output
            import traceback
            traceback.print_exc()
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute a query and return results."""
        with self.transaction() as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
            return []
    
    def execute_insert(self, query: str, params: Optional[tuple] = None) -> Optional[Any]:
        """Execute insert and return inserted ID if available."""
        with self.transaction() as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchone()
            return None
    
    def execute_bulk_insert(self, query: str, params_list: list) -> int:
        """Execute bulk insert and return affected row count."""
        with self.transaction() as cursor:
            cursor.executemany(query, params_list)
            return cursor.rowcount


class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass


# Global database connection instance
_db_connection = None


def get_database_connection() -> DatabaseConnection:
    """Get global database connection instance."""
    global _db_connection
    if _db_connection is None:
        _db_connection = DatabaseConnection()
    return _db_connection


def initialize_database():
    """Initialize database connection and test connectivity."""
    db = get_database_connection()
    if not db.test_connection():
        raise DatabaseError("Failed to establish database connection")
    return db


# Environment configuration helper
def load_database_config_from_env() -> Dict[str, Any]:
    """Load database configuration from environment variables."""
    return {
        'DB_HOST': os.getenv('DB_HOST', 'localhost'),
        'DB_PORT': os.getenv('DB_PORT', '5432'),
        'DB_NAME': os.getenv('DB_NAME', 'fda_spl'),
        'DB_USER': os.getenv('DB_USER', 'postgres'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD', ''),
    }


if __name__ == "__main__":
    # Test database connection
    logging.basicConfig(level=logging.INFO)
    
    print("Testing database connection...")
    config_info = load_database_config_from_env()
    print(f"Configuration: {json.dumps({k: v if k != 'DB_PASSWORD' else '***' for k, v in config_info.items()}, indent=2)}")
    
    try:
        db = initialize_database()
        print("✅ Database connection successful!")
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")