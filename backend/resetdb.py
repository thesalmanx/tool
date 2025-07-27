#!/usr/bin/env python3
"""
Database Reset Script for Partners8 Management System
This script safely removes the old database and creates a fresh one with the updated schema.
"""

import os
import sqlite3
from pathlib import Path

def reset_database():
    """Reset the database by removing the old file and letting the app create a new one"""
    
    db_file = "partners8_data.db"
    
    try:
        if os.path.exists(db_file):
            print(f"🗑️  Removing existing database: {db_file}")
            os.remove(db_file)
            print("✅ Database removed successfully")
        else:
            print("ℹ️  No existing database found")
        
        print("🚀 Database reset complete!")
        print("📝 When you run the application, a new database will be created with:")
        print("   - Default admin user: username=admin, password=admin123")
        print("   - Updated schema with all required columns")
        
    except Exception as e:
        print(f"❌ Error during database reset: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("🔄 Resetting Partners8 Database...")
    print("=" * 50)
    
    if reset_database():
        print("\n✨ Reset completed successfully!")
        print("Now run: python3 main.py")
    else:
        print("\n💥 Reset failed!")