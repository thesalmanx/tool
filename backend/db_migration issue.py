#!/usr/bin/env python3
"""
Database Migration Script for Partners8
Adds missing columns to existing chat_messages table
"""

import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Migrate the database to add missing columns"""
    
    DATABASE_FILE = "partners8_data.db"
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            # Check if chat_messages table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chat_messages'")
            if not cursor.fetchone():
                logger.error("chat_messages table does not exist!")
                return False
            
            # Get current table structure
            cursor.execute("PRAGMA table_info(chat_messages)")
            existing_columns = [column[1] for column in cursor.fetchall()]
            logger.info(f"Existing columns: {existing_columns}")
            
            # Define new columns to add
            new_columns = [
                ("sql_query", "TEXT"),
                ("query_results", "TEXT"), 
                ("query_type", "TEXT DEFAULT 'general'")
            ]
            
            # Add missing columns
            for column_name, column_definition in new_columns:
                if column_name not in existing_columns:
                    try:
                        alter_sql = f"ALTER TABLE chat_messages ADD COLUMN {column_name} {column_definition}"
                        logger.info(f"Adding column: {alter_sql}")
                        cursor.execute(alter_sql)
                        conn.commit()
                        logger.info(f"✅ Successfully added column: {column_name}")
                    except sqlite3.Error as e:
                        logger.error(f"❌ Error adding column {column_name}: {e}")
                        return False
                else:
                    logger.info(f"✅ Column {column_name} already exists")
            
            # Verify the final structure
            cursor.execute("PRAGMA table_info(chat_messages)")
            final_columns = [column[1] for column in cursor.fetchall()]
            logger.info(f"Final columns: {final_columns}")
            
            # Update existing records to have default query_type if it's NULL
            try:
                cursor.execute("UPDATE chat_messages SET query_type = 'general' WHERE query_type IS NULL")
                updated_rows = cursor.rowcount
                conn.commit()
                logger.info(f"✅ Updated {updated_rows} existing records with default query_type")
            except sqlite3.Error as e:
                logger.warning(f"Warning updating existing records: {e}")
            
            logger.info("🎉 Database migration completed successfully!")
            return True
            
    except sqlite3.Error as e:
        logger.error(f"❌ Database migration failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during migration: {e}")
        return False

def verify_migration():
    """Verify that the migration was successful"""
    
    DATABASE_FILE = "partners8_data.db"
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            # Check table structure
            cursor.execute("PRAGMA table_info(chat_messages)")
            columns = cursor.fetchall()
            
            print("\n📋 Current chat_messages table structure:")
            print("-" * 50)
            for column in columns:
                cid, name, type_name, notnull, default_value, pk = column
                print(f"{name:15} | {type_name:10} | Default: {default_value}")
            
            # Check if we can insert a test record
            cursor.execute("SELECT COUNT(*) FROM chat_messages")
            record_count = cursor.fetchone()[0]
            print(f"\n📊 Total records in chat_messages: {record_count}")
            
            # Test insert (rollback immediately)
            try:
                cursor.execute("""
                    INSERT INTO chat_messages 
                    (session_id, message, response, is_grounded, grounding_metadata, 
                     sql_query, query_results, query_type, created_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    999, "test", "test response", False, None, 
                    "SELECT 1", '[]', 'test', datetime.utcnow()
                ))
                # Rollback the test insert
                conn.rollback()
                print("✅ Migration verification successful - can insert with new columns")
                return True
            except sqlite3.Error as e:
                print(f"❌ Migration verification failed: {e}")
                return False
                
    except sqlite3.Error as e:
        print(f"❌ Error verifying migration: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting database migration...")
    
    if migrate_database():
        print("\n🔍 Verifying migration...")
        if verify_migration():
            print("\n🎉 Migration completed and verified successfully!")
        else:
            print("\n⚠️ Migration completed but verification failed")
    else:
        print("\n❌ Migration failed!")