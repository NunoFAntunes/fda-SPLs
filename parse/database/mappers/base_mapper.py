"""
Base mapper class with common functionality for database operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import json

from ..db_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class BaseMapper:
    """Base class for database mappers with common functionality."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.logger = logger.getChild(self.__class__.__name__)
    
    def _convert_value_for_db(self, value: Any) -> Any:
        """Convert Python values to database-compatible formats."""
        if value is None:
            return None
        elif isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return value
        elif isinstance(value, (dict, list)):
            return json.dumps(value, default=self._json_serializer)
        elif isinstance(value, bool):
            return value
        elif isinstance(value, (int, float)):
            return value
        elif hasattr(value, 'value'):  # Handle enums
            return value.value
        else:
            return str(value)
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for complex objects."""
        if hasattr(obj, 'value'):  # Handle enums
            return obj.value
        elif hasattr(obj, '__dict__'):  # Handle objects with dict representation
            return obj.__dict__
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return str(obj)
    
    def _prepare_insert_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for database insertion by converting types."""
        prepared = {}
        for key, value in data.items():
            prepared[key] = self._convert_value_for_db(value)
        return prepared
    
    def _build_insert_query(self, table: str, data: Dict[str, Any]) -> tuple:
        """Build parameterized INSERT query."""
        columns = list(data.keys())
        placeholders = [f"%({col})s" for col in columns]
        
        query = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        """
        
        return query, data
    
    def _build_upsert_query(self, table: str, data: Dict[str, Any], 
                           conflict_columns: List[str], 
                           update_columns: Optional[List[str]] = None) -> tuple:
        """Build parameterized UPSERT (INSERT ... ON CONFLICT) query."""
        if update_columns is None:
            update_columns = [col for col in data.keys() if col not in conflict_columns]
        
        columns = list(data.keys())
        placeholders = [f"%({col})s" for col in columns]
        conflict_clause = ', '.join(conflict_columns)
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        query = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT ({conflict_clause}) 
        DO UPDATE SET {update_clause}
        """
        
        return query, data
    
    def insert_record(self, table: str, data: Dict[str, Any]) -> bool:
        """Insert a single record."""
        try:
            prepared_data = self._prepare_insert_data(data)
            query, params = self._build_insert_query(table, prepared_data)
            
            with self.db.transaction() as cursor:
                cursor.execute(query, params)
                self.logger.debug(f"Inserted record into {table}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to insert into {table}: {str(e)}")
            raise
    
    def upsert_record(self, table: str, data: Dict[str, Any], 
                     conflict_columns: List[str], 
                     update_columns: Optional[List[str]] = None) -> bool:
        """Insert or update a record on conflict."""
        try:
            prepared_data = self._prepare_insert_data(data)
            query, params = self._build_upsert_query(table, prepared_data, 
                                                   conflict_columns, update_columns)
            
            with self.db.transaction() as cursor:
                cursor.execute(query, params)
                self.logger.debug(f"Upserted record into {table}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to upsert into {table}: {str(e)}")
            raise
    
    def insert_batch(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Insert multiple records in a batch."""
        if not records:
            return 0
        
        try:
            # Prepare all records
            prepared_records = [self._prepare_insert_data(record) for record in records]
            
            # Use first record to build query structure
            sample_record = prepared_records[0]
            columns = list(sample_record.keys())
            placeholders = ', '.join([f"%({col})s" for col in columns])
            
            query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({placeholders})
            """
            
            with self.db.transaction() as cursor:
                cursor.executemany(query, prepared_records)
                count = cursor.rowcount
                self.logger.debug(f"Inserted {count} records into {table}")
                return count
                
        except Exception as e:
            self.logger.error(f"Failed to batch insert into {table}: {str(e)}")
            raise
    
    def record_exists(self, table: str, conditions: Dict[str, Any]) -> bool:
        """Check if a record exists with given conditions."""
        try:
            where_clauses = [f"{key} = %({key})s" for key in conditions.keys()]
            query = f"SELECT 1 FROM {table} WHERE {' AND '.join(where_clauses)} LIMIT 1"
            
            with self.db.transaction() as cursor:
                cursor.execute(query, conditions)
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            self.logger.error(f"Failed to check existence in {table}: {str(e)}")
            raise
    
    def get_record(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get a single record matching conditions."""
        try:
            where_clauses = [f"{key} = %({key})s" for key in conditions.keys()]
            query = f"SELECT * FROM {table} WHERE {' AND '.join(where_clauses)} LIMIT 1"
            
            with self.db.transaction() as cursor:
                cursor.execute(query, conditions)
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            self.logger.error(f"Failed to get record from {table}: {str(e)}")
            raise
    
    def delete_related_records(self, table: str, foreign_key: str, foreign_value: Any) -> int:
        """Delete records related to a foreign key value."""
        try:
            query = f"DELETE FROM {table} WHERE {foreign_key} = %(value)s"
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'value': foreign_value})
                count = cursor.rowcount
                self.logger.debug(f"Deleted {count} related records from {table}")
                return count
                
        except Exception as e:
            self.logger.error(f"Failed to delete related records from {table}: {str(e)}")
            raise


class MappingError(Exception):
    """Custom exception for data mapping errors."""
    pass