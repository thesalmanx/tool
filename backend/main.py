import asyncio
import subprocess
import threading
import os
import signal
import time
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, EmailStr
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float # Added Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from passlib.context import CryptContext
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
import jwt
from fastapi import Request
from google import genai
from fastapi import Body
from google.genai import types
from dotenv import load_dotenv
import logging
import uuid
import sqlite3
import pandas as pd
import sys
from sqlalchemy import text
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Database setup
DATABASE_URL = "sqlite:///./partners8_data.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    password_hash = Column(String)
    role = Column(String, default="user")  # "admin" or "user"
    is_approved = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    creator = relationship("User", remote_side=[id], backref="created_users")
    chat_sessions = relationship("ChatSession", back_populates="user")

class ChatSession(Base):
    __tablename__ = "chat_sessions"
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="chat_sessions")
    messages = relationship("ChatMessage", back_populates="session")

class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("chat_sessions.id"))
    message = Column(Text)
    response = Column(Text)
    is_grounded = Column(Boolean, default=False)
    grounding_metadata = Column(Text, nullable=True)  # JSON string
    sql_query = Column(Text, nullable=True)  # Store generated SQL query
    query_results = Column(Text, nullable=True)  # Store SQL results as JSON
    query_type = Column(String, default="general")  # "general", "data_query", "grounded"
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    session = relationship("ChatSession", back_populates="messages")

# Pydantic model for scraping status (used for global state)
# 1. Enhanced ScrapingStatus model with progress fields
class ScrapingStatus(BaseModel):
    status: str  # "idle", "running", "completed", "failed", "stopped", "paused"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    records_processed: Optional[int] = None
    error_message: Optional[str] = None
    current_step: Optional[int] = None
    total_steps: Optional[int] = 6  # Total pipeline steps
    step_name: Optional[str] = None
    progress_percentage: Optional[float] = None

# SQLAlchemy model for scraping logs (persisted in DB)
# 2. Enhanced ScrapingLog model with progress fields
class ScrapingLog(Base):
    __tablename__ = "scraping_logs"
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String)
    started_by = Column(Integer, ForeignKey("users.id"))
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    records_processed = Column(Integer, default=0)
    current_step = Column(Integer, nullable=True)
    total_steps = Column(Integer, default=6)
    step_name = Column(String, nullable=True)
    progress_percentage = Column(Float, nullable=True)


# Global variables for scraping control
scraping_process: Optional[subprocess.Popen] = None
scraping_status = ScrapingStatus(status="idle") # Initialize global status
scraping_thread: Optional[threading.Thread] = None

# 3. Progress file for communication between processes
PROGRESS_FILE = "scraping_progress.json"

def write_progress_file(status: str, current_step: int = None, step_name: str = None,
                        records_processed: int = None, error_message: str = None):
    """Write progress information to file for the main process to read"""
    try:
        progress_data = {
            "status": status,
            "current_step": current_step,
            "total_steps": 6,
            "step_name": step_name,
            "records_processed": records_processed,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat(),
            "progress_percentage": (current_step / 6 * 100) if current_step else None
        }

        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f)
    except Exception as e:
        logger.error(f"Failed to write progress file: {e}")

def read_progress_file():
    """Read progress information from file"""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read progress file: {e}")
    return None

def cleanup_progress_file():
    """Clean up progress file"""
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    except Exception as e:
        logger.error(f"Failed to cleanup progress file: {e}")

# 4. Enhanced scraping process function
def run_scraping_script(user_id: int):
    """Run the scraping script in a separate process with progress tracking"""
    global scraping_process, scraping_status

    try:
        # Update status to running
        scraping_status.status = "running"
        scraping_status.started_at = datetime.now()
        scraping_status.completed_at = None
        scraping_status.error_message = None
        scraping_status.records_processed = 0
        scraping_status.current_step = 0
        scraping_status.total_steps = 6
        scraping_status.step_name = "Initializing"

        # Write initial progress
        try:
            write_progress_file("running", 0, "Initializing", 0)
        except:
            pass
        # print the current working directory
   


        # Start the scraper script
        try:
            script_path = "scrape.py"
            scraping_process = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
        except Exception as e:
            print(f"Error starting scraping script: {e}")
            logger.error(f"Error starting scraping script: {e}")
            scraping_status.status = "failed"

        logger.info(f"📍 Started scraping process with PID: {scraping_process.pid}")

        # Monitor the process and read progress
        while scraping_process is not None and scraping_process.poll() is None:
            # Check if we should stop
            if scraping_status.status == "stopped":
                break
                
            # Read progress from file
            try:
                progress_data = read_progress_file()
                if progress_data:
                    scraping_status.current_step = progress_data.get("current_step")
                    scraping_status.step_name = progress_data.get("step_name")
                    scraping_status.records_processed = progress_data.get("records_processed")
                    scraping_status.progress_percentage = progress_data.get("progress_percentage")
                    
                    # Update status if changed
                    new_status = progress_data.get("status")
                    if new_status and new_status != scraping_status.status and new_status != "stopped":
                        scraping_status.status = new_status
            except:
                pass  # Ignore file read errors
            
            time.sleep(1)  # Check every second

        # Handle completion
        if scraping_process is not None:
            try:
                # Get final results
                return_code = scraping_process.returncode
                if return_code is None:
                    # Process might still be running, try to get return code
                    scraping_process.wait(timeout=5)
                    return_code = scraping_process.returncode
                
                # Update final status
                scraping_status.completed_at = datetime.now()

                if return_code == 0:
                    scraping_status.status = "completed"
                    # Try to get final record count
                    try:
                        if os.path.exists("partners8_final_data.csv"):
                            import pandas as pd
                            df = pd.read_csv("partners8_final_data.csv")
                            scraping_status.records_processed = len(df)
                    except:
                        pass
                else:
                    if scraping_status.status != "stopped":
                        scraping_status.status = "failed"
                        scraping_status.error_message = "Process ended with non-zero exit code"
            except:
                if scraping_status.status != "stopped":
                    scraping_status.status = "failed"
                    scraping_status.error_message = "Error getting process result"

        # Log final status to DB
        log_scraping_operation(
            user_id,
            scraping_status.status,
            scraping_status.error_message,
            scraping_status.records_processed,
            scraping_status.current_step,
            scraping_status.step_name
        )

        # Cleanup
        try:
            cleanup_progress_file()
        except:
            pass
            
        scraping_process = None

    except Exception as e:
        scraping_status.status = "failed"
        scraping_status.completed_at = datetime.now()
        scraping_status.error_message = str(e)
        try:
            cleanup_progress_file()
        except:
            pass
        scraping_process = None
        logger.error(f"❌ Scraping script error: {e}")


def stop_scraping_process():
    """Force stop the running scraping process"""
    global scraping_process, scraping_status
    
    logger.info("🛑 Stopping scraping process...")
    
    # Update status immediately
    scraping_status.status = "stopped"
    scraping_status.completed_at = datetime.now()
    
    # Write stop signal to progress file
    try:
        write_progress_file("stopped", getattr(scraping_status, 'current_step', 0), "Pipeline stopped by user")
    except:
        pass
    
    # Kill the process if it exists
    if scraping_process is not None:
        try:
            # Just kill it - no checking, no waiting
            scraping_process.kill()
            logger.info("✅ Process killed")
        except:
            pass  # Ignore any errors, process might already be dead
    
    # Always reset to None
    scraping_process = None
    
    # Nuclear option: kill via system commands
    try:
        import os
        import platform
        
        if platform.system() == "Windows":
            os.system('taskkill /f /im python.exe >nul 2>&1')
        else:
            os.system('pkill -f scrape.py >/dev/null 2>&1')
    except:
        pass  # Ignore errors
    
    logger.info("🛑 Stop completed")

# 5. Enhanced log_scraping_operation function
def log_scraping_operation(user_id: int, status: str, error_message: Optional[str] = None,
                           records_processed: int = 0, current_step: int = None,
                           step_name: str = None):
    """Log scraping operation to database with progress info"""
    try:
        conn = sqlite3.connect("partners8_data.db")
        cursor = conn.cursor()
        created_at=datetime.now()

        if status == "started":
            cursor.execute('''
                INSERT INTO scraping_logs (
                    status, started_by,started_at, records_processed, current_step,
                    total_steps, step_name, progress_percentage
                )
                VALUES (?, ?, ?, ?,?, ?, ?, ?)
            ''', ("running", user_id,created_at, records_processed, current_step, 6, step_name,(current_step / 6 * 100) if current_step else 0))
        else:
            # Update the most recent log entry
            cursor.execute('''
                UPDATE scraping_logs
                SET status = ?, completed_at = CURRENT_TIMESTAMP,
                    error_message = ?, records_processed = ?,
                    current_step = ?, step_name = ?, progress_percentage = ?
                WHERE id = (
                    SELECT id FROM scraping_logs
                    WHERE started_by = ?
                    ORDER BY started_at DESC
                    LIMIT 1
                )
            ''', (status, error_message, records_processed, current_step, step_name,
                  (current_step / 6 * 100) if current_step else 0, user_id))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error logging scraping operation: {e}") # Changed print to logger.error

# Database table creation (add this to your database setup)
# 6. Enhanced database table creation
def create_scraping_tables():
    """Create scraping-related tables with progress fields"""
    conn = sqlite3.connect("partners8_data.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scraping_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL,
            started_by INTEGER NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP NULL,
            error_message TEXT NULL,
            records_processed INTEGER DEFAULT 0,
            current_step INTEGER NULL,
            total_steps INTEGER DEFAULT 6,
            step_name TEXT NULL,
            progress_percentage REAL NULL,
            FOREIGN KEY (started_by) REFERENCES users (id)
        )
    ''')
    conn.commit()
    conn.close()

# Create tables and handle migrations
def create_tables_and_migrate():
    """Create tables and handle database migrations"""
    Base.metadata.create_all(bind=engine)
    create_scraping_tables() # Call the new function here

    db = SessionLocal()
    try:
        # Check if updated_at column exists in users table
        try:
            db.execute(text("SELECT updated_at FROM users LIMIT 1"))
        except Exception:
            try:
                db.execute(text('ALTER TABLE users ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP'))
                db.commit()
                logger.info("Added updated_at column to users table")
            except Exception as e:
                logger.error(f"Error adding updated_at column to users: {e}")

        # Add new columns to chat_messages table
        try:
            db.execute(text("SELECT sql_query FROM chat_messages LIMIT 1"))
        except Exception:
            try:
                db.execute(text('ALTER TABLE chat_messages ADD COLUMN sql_query TEXT'))
                db.execute(text('ALTER TABLE chat_messages ADD COLUMN query_results TEXT'))
                db.execute(text('ALTER TABLE chat_messages ADD COLUMN query_type TEXT DEFAULT \'general\''))
                db.commit()
                logger.info("Added new columns to chat_messages table")
            except Exception as e:
                logger.error(f"Error adding columns to chat_messages table: {e}")

        # Add new columns to scraping_logs table if they don't exist
        # This is for existing databases that might not have the new progress columns
        try:
            db.execute(text("SELECT current_step FROM scraping_logs LIMIT 1"))
        except Exception:
            try:
                db.execute(text('ALTER TABLE scraping_logs ADD COLUMN current_step INTEGER NULL'))
                db.execute(text('ALTER TABLE scraping_logs ADD COLUMN total_steps INTEGER DEFAULT 6'))
                db.execute(text('ALTER TABLE scraping_logs ADD COLUMN step_name TEXT NULL'))
                db.execute(text('ALTER TABLE scraping_logs ADD COLUMN progress_percentage REAL NULL'))
                db.commit()
                logger.info("Added new progress columns to scraping_logs table")
            except Exception as e:
                logger.error(f"Error adding progress columns to scraping_logs: {e}")

        # Check and create other tables if needed
        inspector = engine.dialect.get_table_names(db.connection())
        required_tables = ['chat_sessions', 'chat_messages', 'scraping_logs']

        for table in required_tables:
            if table not in inspector:
                logger.info(f"Creating missing table: {table}")

    except Exception as e:
        logger.error(f"Migration error: {e}")
    finally:
        db.close()

create_tables_and_migrate()


# Security and Authentication
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Authentication dependencies
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception

    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exception
    if not user.is_approved:
        raise HTTPException(status_code=403, detail="User account not approved")
    return user

def get_current_admin_user(current_user: User = Depends(get_current_user)):
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Operation not permitted - Admin access required")
    return current_user

# Google AI Client Setup
def get_genai_client():
    """Initialize and return the Google GenAI client"""
    try:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logger.error("GEMINI_API_KEY not found in environment variables")
            return None
        client = genai.Client(api_key=api_key)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize GenAI client: {e}")
        return None

# Database Schema Helper
def get_database_schema():
    """Get the database schema information for partners8_data table"""
    try:
        with sqlite3.connect("partners8_data.db") as conn:
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='partners8_data'")
            if cursor.fetchone() is None:
                return None

            cursor.execute("PRAGMA table_info(partners8_data)")
            columns = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM partners8_data")
            total_rows = cursor.fetchone()[0]

            return {
                'columns': columns,
                'total_rows': total_rows
            }
    except Exception as e:
        logger.error(f"Failed to get database schema: {e}")
        return None

def create_schema_prompt():
    """Create a detailed schema prompt for Gemini"""
    schema_data = get_database_schema()
    if not schema_data:
        return None

    column_descriptions = {
        'id': 'Primary key, auto-increment',
        'ZipCode': 'Zillow ZipCode ID',
        'SizeRank': 'City size ranking by population',
        'RegionName': 'City name',
        'State': 'US State abbreviation (e.g., CA, TX, NY)',
        'County': 'County name',
        'City': 'City name (same as RegionName)',
        'ZMediumRent': 'Zillow median rent price in USD',
        'ZMediumValue': 'Zillow median home value in USD',
        'NMediumValue': 'NAR (Census) median home value in USD',
        'entityid': 'HUD FIPS code for the area',
        'IncomeLimits': 'HUD income limits for very low income (50% AMI, 4-person household)',
        'Efficiency': 'HUD Fair Market Rent for efficiency apartment',
        'OneBedroom': 'HUD Fair Market Rent for 1-bedroom apartment',
        'TwoBedroom': 'HUD Fair Market Rent for 2-bedroom apartment',
        'ThreeBedroom': 'HUD Fair Market Rent for 3-bedroom apartment',
        'FourBedroom': 'HUD Fair Market Rent for 4-bedroom apartment',
        'ZillowRatio': 'Monthly rent to home value ratio (Zillow data)',
        'NARRatio': 'Monthly rent to home value ratio (NAR data)',
        'ZH Ratio': 'HUD 4-bedroom rent to Zillow home value ratio',
        'NH Ratio': 'HUD 4-bedroom rent to NAR home value ratio',
        'created_at': 'Record creation timestamp',
        'updated_at': 'Record update timestamp'
    }

    schema_text = "DATABASE SCHEMA FOR PARTNERS 8 REAL ESTATE DATA:\n\n"
    schema_text += f"Table: partners8_data (Total rows: {schema_data['total_rows']:,})\n\nColumns:\n"

    for col in schema_data['columns']:
        col_name = col[1]
        col_type = col[2]
        description = column_descriptions.get(col_name, 'Real estate data field')
        schema_text += f"- {col_name} ({col_type}): {description}\n"

    schema_text += "\nIMPORTANT NOTES:\n"
    schema_text += "1. Use SQLite syntax\n"
    schema_text += "2. All monetary values are in USD\n"
    schema_text += "3. State codes are 2-letter abbreviations\n"
    schema_text += "4. NULL values may exist in any column\n"
    schema_text += "5. Ratios are decimal values (e.g., 0.01 = 1%)\n"
    schema_text += "6. Only query the 'partners8_data' table\n"
    schema_text += "7. Use proper WHERE clauses for filtering\n"
    schema_text += "8. Consider using LIMIT for large result sets\n"
    schema_text += "9. Use double quotes for column names with spaces like \"ZH Ratio\"\n"

    return schema_text

# Smart Query Router
def is_data_query(message: str) -> bool:
    """Determine if the message is asking for data analysis"""
    data_keywords = [
        'show', 'find', 'get', 'list', 'what are', 'which', 'how many', 'count',
        'average', 'median', 'highest', 'lowest', 'top', 'bottom', 'compare',
        'rent', 'price', 'value', 'income', 'city', 'state', 'expensive', 'cheap',
        'affordable', 'ratio', 'bedroom', 'apartment', 'housing', 'real estate',
        'zillow', 'hud', 'market', 'analysis', 'data', 'statistics', 'stats'
    ]

    message_lower = message.lower()
    return any(keyword in message_lower for keyword in data_keywords)

# Enhanced Chat Functions
async def natural_language_to_sql(user_question: str) -> Dict[str, Any]:
    """Convert natural language question to SQL query using Gemini"""
    schema_prompt = create_schema_prompt()
    if not schema_prompt:
        return {"success": False, "error": "Database schema not available"}

    prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"

Please convert this natural language question into a valid SQLite SQL query for the partners8_data table.

REQUIREMENTS:
1. Generate ONLY the SQL query, no explanations or markdown
2. Use proper SQLite syntax
3. Include appropriate WHERE clauses if filtering is needed
4. Use ORDER BY and LIMIT when appropriate
5. Handle potential NULL values properly
6. Make sure column names match exactly (case-sensitive, use double quotes for columns with spaces)
7. Start directly with SELECT


Generate a complete, executable SQL query:
"""

    try:
        client = get_genai_client()
        if not client:
            return {"success": False, "error": "Failed to initialize AI client"}

        config = types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=500,
        )

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=config,
        )

        sql_query = clean_sql_query(response.text)
        return {"success": True, "sql_query": sql_query}

    except Exception as e:
        logger.error(f"Error generating SQL query: {e}")
        return {"success": False, "error": str(e)}

def clean_sql_query(sql_query: str) -> str:
    """Clean up the SQL query response from Gemini"""
    sql_query = sql_query.strip()

    if sql_query.startswith('```sql'):
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
    elif sql_query.startswith('```'):
        sql_query = sql_query.replace('```', '').strip()

    lines = sql_query.split('\n')
    sql_lines = []
    found_select = False

    for line in lines:
        line = line.strip()
        if line.upper().startswith('SELECT') or found_select:
            found_select = True
            sql_lines.append(line)
        elif any(keyword in line.upper() for keyword in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'HAVING']):
            sql_lines.append(line)

    if sql_lines:
        sql_query = '\n'.join(sql_lines)

    return sql_query

async def execute_sql_query(sql_query: str) -> Dict[str, Any]:
    """Execute the SQL query and return results"""
    try:
        with sqlite3.connect("partners8_data.db") as conn:
            df = pd.read_sql_query(sql_query, conn)

            # Convert DataFrame to list of dictionaries for JSON serialization
            results = df.to_dict('records')

            return {
                "success": True,
                "results": results,
                "row_count": len(results),
                "columns": list(df.columns)
            }
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        return {"success": False, "error": str(e)}

async def summarize_query_results(user_question: str, sql_query: str, results: List[Dict]) -> str:
    """Use Gemini to summarize the query results"""
    if len(results) == 0:
        return "No results found for your query."

    # Limit data for summary to avoid token limits
    display_results = results[:10]
    results_text = json.dumps(display_results, indent=2, default=str)

    if len(results) > 10:
        results_text += f"\n... and {len(results) - 10} more rows"

    prompt = f"""
ORIGINAL QUESTION: "{user_question}"
SQL QUERY: {sql_query}
RESULTS ({len(results)} total rows): {results_text}

Provide a clear, concise summary of these results in 2-3 sentences. Focus on key insights and patterns. Include specific numbers and findings.
"""

    try:
        client = get_genai_client()
        if not client:
            return "Results found but unable to generate summary due to AI client error."

        config = types.GenerateContentConfig(
            temperature=0.3,
            max_output_tokens=300,
        )

        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=config,
        )

        return response.text.strip()
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return f"Results found ({len(results)} rows) but unable to generate summary: {str(e)}"

# Enhanced Google Grounding Search Function
async def search_with_google_grounding(query: str) -> Dict[str, Any]:
    """Search using Google Grounding API with the new library"""
    try:
        client = get_genai_client()
        if not client:
            raise Exception("Failed to initialize GenAI client")

        # Define the grounding tool using the new API
        grounding_tool = types.Tool(
            google_search=types.GoogleSearch()
        )

        # Configure generation settings
        config = types.GenerateContentConfig(
            tools=[grounding_tool],
            temperature=0.7,
            max_output_tokens=1000,
        )

        # Make the request with grounding
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=query,
            config=config,
        )

        # Check if grounding metadata exists
        grounding_metadata = None
        sources = []
        is_grounded = False

        if (response.candidates and
            len(response.candidates) > 0 and
            hasattr(response.candidates[0], 'grounding_metadata') and
            response.candidates[0].grounding_metadata):

            is_grounded = True
            grounding_meta = response.candidates[0].grounding_metadata

            # Extract sources from grounding chunks
            if hasattr(grounding_meta, 'grounding_chunks') and grounding_meta.grounding_chunks:
                for chunk in grounding_meta.grounding_chunks:
                    if hasattr(chunk, 'web') and chunk.web:
                        sources.append({
                            "title": chunk.web.title if hasattr(chunk.web, 'title') else "Unknown",
                            "uri": chunk.web.uri if hasattr(chunk.web, 'uri') else "",
                        })

            # Store the full grounding metadata as JSON string
            grounding_metadata = {
                "web_search_queries": grounding_meta.web_search_queries if hasattr(grounding_meta, 'web_search_queries') else [],
                "grounding_chunks_count": len(grounding_meta.grounding_chunks) if hasattr(grounding_meta, 'grounding_chunks') else 0,
                "sources_count": len(sources)
            }

        return {
            "response": response.text,
            "is_grounded": is_grounded,
            "sources": sources,
            "grounding_metadata": json.dumps(grounding_metadata) if grounding_metadata else None
        }

    except Exception as e:
        logger.error(f"Google AI search error: {e}")

        # Fallback to non-grounded response
        try:
            client = get_genai_client()
            if not client:
                raise Exception("Failed to initialize GenAI client for fallback")

            # Use regular generation without grounding
            config = types.GenerateContentConfig(
                temperature=0.7,
                max_output_tokens=1000,
            )

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"Please provide a helpful response to this query: {query}",
                config=config,
            )

            return {
                "response": response.text,
                "is_grounded": False,
                "sources": [],
                "grounding_metadata": None
            }

        except Exception as fallback_error:
            logger.error(f"Fallback AI response error: {fallback_error}")
            return {
                "response": f"I apologize, but I encountered an error while processing your request. Please try again later.",
                "is_grounded": False,
                "sources": [],
                "grounding_metadata": None
            }

# Background task for scraping (This is the old version, replaced by run_scraping_script)
# This function is not used with the new progress tracking approach.
# async def run_scraping_task(user_id: int):
#     """Background task to run scraping"""
#     db = SessionLocal()
#     try:
#         # Create scraping log
#         log = ScrapingLog(
#             status="running",
#             started_by=user_id
#         )
#         db.add(log)
#         db.commit()

#         # Run scraping script
#         process = subprocess.Popen(
#             ["python3", "scrape.py"],
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE
#         )

#         stdout, stderr = process.communicate()

#         # Update log
#         log.completed_at = datetime.utcnow()
#         if process.returncode == 0:
#             log.status = "completed"
#             log.records_processed = 100  # You can parse actual count from stdout
#         else:
#             log.status = "failed"
#             log.error_message = stderr.decode()

#         db.commit()

#     except Exception as e:
#         logger.error(f"Scraping task error: {e}")
#         if 'log' in locals():
#             log.status = "failed"
#             log.error_message = str(e)
#             log.completed_at = datetime.utcnow()
#             db.commit()
#     finally:
#         db.close()

# Utility functions
def create_first_admin():
    """Create the first admin user if no users exist"""
    db = SessionLocal()
    try:
        # Check if any admin users exist
        admin_count = db.query(User).filter(User.role == "admin").count()
        if admin_count == 0:
            admin_user = User(
                username="admin",
                email="admin@example.com",
                password_hash=get_password_hash("admin123"),
                role="admin",
                is_approved=True
            )
            db.add(admin_user)
            db.commit()
            logger.info("First admin user created: username=admin, password=admin123")
    except Exception as e:
        logger.error(f"Error creating first admin: {e}")
        db.rollback()
    finally:
        db.close()

# Pydantic models for API requests/responses
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str

class AdminUserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    role: str = "user"
    is_approved: bool = True

class UserOut(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_approved: bool
    created_at: datetime
    created_by: Optional[int]

    class Config:
        from_attributes = True

class UserUpdate(BaseModel):
    role: Optional[str] = None
    is_approved: Optional[bool] = None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str
    is_grounded: bool = False
    sources: Optional[List[Dict[str, Any]]] = None
    query_type: str = "general"
    sql_query: Optional[str] = None
    query_results: Optional[List[Dict[str, Any]]] = None

# FastAPI lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    create_first_admin()
    logger.info("Application started successfully")
    yield
    # Shutdown
    logger.info("Application shutting down")

# FastAPI app instance
# IMPORTANT: The 'app' instance must be created before any @app.on_event or route decorators.
app = FastAPI(
    title="Partners8 Management System",
    description="A comprehensive system with user management, AI chat with data query capabilities, and data scraping",
    version="2.0.0",
    lifespan=lifespan,
    # docs_url=None,s
    redoc_url=None
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # Next.js development server
        "http://localhost:8000",  # If frontend is served by FastAPI in development
        "https://investmentapp.partners8.com" # Your production domain
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# FastAPI event handlers
@app.on_event("startup")
async def startup_event():
    """Initialize scraping tables and cleanup any orphaned processes"""
    # Reset any running status on startup (in case of server restart)
    global scraping_status
    scraping_status = ScrapingStatus(status="idle")
    cleanup_progress_file() # Ensure a clean slate on startup

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup scraping processes on shutdown"""
    global scraping_process
    if scraping_process and scraping_process.poll() is None:
        stop_scraping_process()
    cleanup_progress_file() # Final cleanup on shutdown

# API Endpoints

# Authentication endpoints
@app.post("/token")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_approved:
        raise HTTPException(status_code=403, detail="User account not approved")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role
        }
    }

@app.post("/signup")
async def signup(user: UserCreate, db: Session = Depends(get_db)):
    # Check if user already exists
    existing_user = db.query(User).filter(
        (User.username == user.username) | (User.email == user.email)
    ).first()

    if existing_user:
        raise HTTPException(status_code=400, detail="Username or email already registered")

    # Create new user
    hashed_password = get_password_hash(user.password)
    new_user = User(
        username=user.username,
        email=user.email,
        password_hash=hashed_password,
        role="user",
        is_approved=False
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {"message": "User created successfully. Awaiting admin approval."}

@app.get("/verify-token")
async def verify_token(current_user: User = Depends(get_current_user)):
    return {
        "valid": True,
        "user": {
            "id": current_user.id,
            "username": current_user.username,
            "email": current_user.email,
            "role": current_user.role,
            "is_approved": current_user.is_approved
        }
    }

# User management endpoints
@app.post("/users", response_model=dict)
async def create_user(
    user: AdminUserCreate,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    existing_user = db.query(User).filter(
        (User.username == user.username) | (User.email == user.email)
    ).first()

    if existing_user:
        raise HTTPException(status_code=400, detail="Username or email already registered")

    hashed_password = get_password_hash(user.password)
    new_user = User(
        username=user.username,
        email=user.email,
        password_hash=hashed_password,
        role=user.role,
        is_approved=user.is_approved,
        created_by=current_user.id
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {"message": "User created successfully", "user_id": new_user.id}

@app.get("/users", response_model=dict)
async def get_users(
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 10
):
    """Get paginated list of users"""
    offset = (page - 1) * limit
    total_users = db.query(User).count()
    users = db.query(User).offset(offset).limit(limit).all()
    return {"total": total_users, "page": page, "limit": limit, "users": [UserOut.from_orm(user) for user in users]}

@app.put("/users/{user_id}")
async def update_user(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user_update.role is not None:
        user.role = user_update.role
    if user_update.is_approved is not None:
        user.is_approved = user_update.is_approved

    # Only update updated_at if the column exists
    try:
        user.updated_at = datetime.utcnow()
    except:
        pass  # Column doesn't exist in older schema

    db.commit()

    return {"message": "User updated successfully"}

@app.put("/approve_user/{user_id}")
async def approve_user(
    user_id: int,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_approved = True
    try:
        user.updated_at = datetime.utcnow()
    except:
        pass  # Column doesn't exist in older schema
    db.commit()

    return {"message": "User approved successfully"}

@app.put("/promote_to_admin/{user_id}")
async def promote_to_admin(
    user_id: int,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = "admin"
    try:
        user.updated_at = datetime.utcnow()
    except:
        pass  # Column doesn't exist in older schema
    db.commit()

    return {"message": f"User {user.username} promoted to admin successfully"}

# Enhanced Chat endpoint with integrated SQL query capabilities
@app.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        # Get or create chat session
        if request.session_id:
            session = db.query(ChatSession).filter(
                ChatSession.session_id == request.session_id,
                ChatSession.user_id == current_user.id
            ).first()
        else:
            session = None

        if not session:
            session = ChatSession(user_id=current_user.id)
            db.add(session)
            db.commit()
            db.refresh(session)

        # Determine query type and route accordingly
        if is_data_query(request.message):
            # Handle as data query
            query_type = "data_query"
            sql_result = await natural_language_to_sql(request.message)

            if sql_result["success"]:
                # Execute the SQL query
                execution_result = await execute_sql_query(sql_result["sql_query"])

                if execution_result["success"]:
                    # Generate summary of results
                    summary = await summarize_query_results(
                        request.message,
                        sql_result["sql_query"],
                        execution_result["results"]
                    )

                    # Create response with data
                    response_text = f"**Data Analysis Results:**\n\n{summary}"
                    if execution_result["row_count"] > 0:
                        response_text += f"\n\n**Found {execution_result['row_count']} records matching your query.**"

                        # Add some sample data if available
                        if len(execution_result["results"]) > 0:
                            sample_count = min(3, len(execution_result["results"]))
                            response_text += f"\n\n**Sample Results (showing {sample_count} of {execution_result['row_count']}):**\n"
                            for i, result in enumerate(execution_result["results"][:sample_count]):
                                response_text += f"\n{i+1}. "
                                # Format key fields nicely
                                key_fields = ['ZipCode','RegionName', 'State', 'ZMediumRent', 'ZMediumValue', 'IncomeLimits']
                                displayed_fields = []
                                for field in key_fields:
                                    if field in result and result[field] is not None:
                                        value = result[field]
                                        if isinstance(value, (int, float)) and field != 'State':
                                            value = f"${value:,.0f}" if value > 1000 else str(value)
                                        displayed_fields.append(f"{field}: {value}")
                                response_text += ", ".join(displayed_fields[:3])

                    # Save chat message with SQL data
                    chat_message = ChatMessage(
                        session_id=session.id,
                        message=request.message,
                        response=response_text,
                        is_grounded=False,
                        grounding_metadata=None,
                        sql_query=sql_result["sql_query"],
                        query_results=json.dumps(execution_result["results"]),
                        query_type=query_type
                    )
                    db.add(chat_message)
                    db.commit()

                    return ChatResponse(
                        response=response_text,
                        session_id=session.session_id,
                        is_grounded=False,
                        sources=[],
                        query_type=query_type,
                        sql_query=sql_result["sql_query"],
                        query_results=execution_result["results"]
                    )
                else:
                    # SQL execution failed, fall back to grounded search
                    grounded_result = await search_with_google_grounding(request.message)
                    response_text = f"I couldn't query the database directly (SQL error), but here's what I found online:\n\n{grounded_result['response']}"
                    query_type = "grounded_fallback"
            else:
                # SQL generation failed, fall back to grounded search
                grounded_result = await search_with_google_grounding(request.message)
                response_text = f"I couldn't generate a database query for that question, but here's what I found online:\n\n{grounded_result['response']}"
                query_type = "grounded_fallback"
        else:
            # Handle as general query with grounding
            query_type = "grounded"
            grounded_result = await search_with_google_grounding(request.message)
            response_text = grounded_result["response"]

        # For non-data queries or fallback cases
        if query_type in ["grounded", "grounded_fallback"]:
            # Save chat message with grounding data
            chat_message = ChatMessage(
                session_id=session.id,
                message=request.message,
                response=response_text,
                is_grounded=grounded_result["is_grounded"],
                grounding_metadata=grounded_result["grounding_metadata"],
                sql_query=None,
                query_results=None,
                query_type=query_type
            )
            db.add(chat_message)
            db.commit()

            return ChatResponse(
                response=response_text,
                session_id=session.session_id,
                is_grounded=grounded_result["is_grounded"],
                sources=grounded_result["sources"],
                query_type=query_type,
                sql_query=None,
                query_results=None
            )

        # Update session timestamp
        try:
            session.updated_at = datetime.utcnow()
            db.commit()
        except Exception as e:
            logger.error(f"Error updating session timestamp: {e}")
            pass  # Column doesn't exist in older schema

    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during chat processing")

# Scraping endpoints
# 7. Enhanced get_scraping_status endpoint

@app.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    """Delete a user account (Admin only)"""
    try:
        # Find the user to delete
        user_to_delete = db.query(User).filter(User.id == user_id).first()
        if not user_to_delete:
            raise HTTPException(status_code=404, detail="User not found")

        # Security checks
        # 1. Prevent self-deletion
        if user_to_delete.id == current_user.id:
            raise HTTPException(status_code=400, detail="You cannot delete your own account")

        # 2. Only super_admin can delete other admins
        if user_to_delete.role == "admin" and current_user.role != "super_admin":
            raise HTTPException(
                status_code=403, 
                detail="Only super admins can delete admin accounts"
            )

        # 3. Prevent deletion of the last admin/super_admin
        if user_to_delete.role in ["admin", "super_admin"]:
            admin_count = db.query(User).filter(User.role.in_(["admin", "super_admin"])).count()
            if admin_count <= 1:
                raise HTTPException(
                    status_code=400, 
                    detail="Cannot delete the last admin account. System must have at least one admin."
                )

        # Store user info for logging before deletion
        deleted_username = user_to_delete.username
        deleted_role = user_to_delete.role

        # Delete related data first (if any)
        # Delete chat sessions and messages
        try:
            chat_sessions = db.query(ChatSession).filter(ChatSession.user_id == user_id).all()
            for session in chat_sessions:
                # Delete messages in this session
                db.query(ChatMessage).filter(ChatMessage.session_id == session.id).delete()
                # Delete the session
                db.delete(session)
            
            logger.info(f"Deleted {len(chat_sessions)} chat sessions for user {deleted_username}")
        except Exception as e:
            logger.warning(f"Error deleting chat data for user {user_id}: {e}")
            # Continue with user deletion even if chat cleanup fails

        # Delete scraping logs started by this user (optional - you might want to keep these for auditing)
        try:
            scraping_logs_deleted = db.query(ScrapingLog).filter(ScrapingLog.started_by == user_id).delete()
            logger.info(f"Deleted {scraping_logs_deleted} scraping logs for user {deleted_username}")
        except Exception as e:
            logger.warning(f"Error deleting scraping logs for user {user_id}: {e}")

        # Delete the user
        db.delete(user_to_delete)
        db.commit()

        logger.info(f"Admin {current_user.username} deleted user {deleted_username} (role: {deleted_role})")

        return {
            "message": f"User '{deleted_username}' deleted successfully",
            "deleted_user": {
                "username": deleted_username,
                "role": deleted_role
            },
            "deleted_by": current_user.username
        }

    except HTTPException:
        # Re-raise HTTP exceptions (like 404, 403, etc.)
        raise
    except Exception as e:
        logger.error(f"Error deleting user {user_id}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete user: {str(e)}")
    


@app.get("/admin/deletion_audit")
async def get_deletion_audit(
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    """Get audit log of deleted users (from application logs)"""
    # Since we're logging deletions, you could read from log files
    # or implement a separate audit table for better tracking
    
    # For now, return a simple response
    return {
        "message": "User deletion audit logs are available in application logs",
        "note": "Consider implementing a dedicated audit table for better tracking"
    }


@app.get("/scraping_status")
async def get_scraping_status(current_user: dict = Depends(get_current_user)):
    """Get current scraping status with progress information"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    # Read latest progress from file if scraping is running
    # This ensures the most up-to-date progress from the subprocess
    if scraping_status.status == "running":
        progress_data = read_progress_file()
        if progress_data:
            scraping_status.current_step = progress_data.get("current_step")
            scraping_status.step_name = progress_data.get("step_name")
            scraping_status.records_processed = progress_data.get("records_processed")
            scraping_status.progress_percentage = progress_data.get("progress_percentage")
            # Update status if changed by the subprocess (e.g., "paused", "completed", "failed")
            new_status = progress_data.get("status")
            if new_status and new_status != scraping_status.status:
                scraping_status.status = new_status
            if progress_data.get("error_message"):
                scraping_status.error_message = progress_data.get("error_message")


    return {
        "status": scraping_status.status,
        "started_at": scraping_status.started_at.isoformat() if scraping_status.started_at else None,
        "completed_at": scraping_status.completed_at.isoformat() if scraping_status.completed_at else None,
        "records_processed": scraping_status.records_processed,
        "error_message": scraping_status.error_message,
        "current_step": scraping_status.current_step,
        "total_steps": scraping_status.total_steps,
        "step_name": scraping_status.step_name,
        "progress_percentage": scraping_status.progress_percentage
    }


@app.get("/scraping_logs")
async def get_scraping_logs(
    current_user: dict = Depends(get_current_user),
    page: int = 1,
    limit: int = 10
):
    """Get paginated scraping history logs"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        conn = sqlite3.connect("partners8_data.db")
        cursor = conn.cursor()

        offset = (page - 1) * limit

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM scraping_logs")
        total_logs = cursor.fetchone()[0]

        cursor.execute('''
            SELECT id, status, started_by, started_at, completed_at, error_message, records_processed,
                   current_step, total_steps, step_name, progress_percentage
            FROM scraping_logs
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))

        logs = []
        for row in cursor.fetchall():
            log = {
                "id": row[0],
                "status": row[1],
                "started_by": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "error_message": row[5],
                "records_processed": row[6] or 0,
                "current_step": row[7],
                "total_steps": row[8],
                "step_name": row[9],
                "progress_percentage": row[10]
            }
            logs.append(log)
        conn.close()
        return {"total": total_logs, "page": page, "limit": limit, "logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/start_scraping")
async def start_scraping(current_user: dict = Depends(get_current_user)):
    """Start the scraping process"""
    global scraping_thread, scraping_status

    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    # Check if already running
    if scraping_status.status == "running":
        raise HTTPException(status_code=400, detail="Scraping is already running")

    try:
        # Log the start
        log_scraping_operation(current_user.id, "started")

        # Start scraping in a separate thread
        scraping_thread = threading.Thread(target=run_scraping_script, args=(current_user.id,), daemon=True)
        scraping_thread.start()

        return {"message": "Scraping started successfully", "status": "running"}

    except Exception as e:
        scraping_status.status = "failed"
        scraping_status.error_message = str(e)
        log_scraping_operation(current_user.id, "failed", str(e))
        raise HTTPException(status_code=500, detail=f"Failed to start scraping: {str(e)}")

@app.post("/stop_scraping")
async def stop_scraping(current_user: dict = Depends(get_current_user)):
    """Stop the scraping process"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    # Don't check if running - just try to stop
    try:
        stop_scraping_process()
        log_scraping_operation(
            current_user.id,
            "stopped",
            records_processed=getattr(scraping_status, 'records_processed', 0)
        )

        return {"message": "Stop signal sent successfully", "status": "stopped"}

    except Exception as e:
        logger.error(f"❌ Stop error: {e}")
        return {"message": "Stop attempted", "status": "stopped"}  # Return success anyway
# Removed pause_scraping and resume_scraping as control is now via progress file in scrape.py
# @app.post("/pause_scraping")
# async def pause_scraping(current_user: dict = Depends(get_current_user)):
#     """Pause the scraping process"""
#     global scraping_process

#     if current_user.role != "admin":
#         raise HTTPException(status_code=403, detail="Admin access required")

#     if scraping_status.status != "running":
#         raise HTTPException(status_code=400, detail="No scraping process is currently running")

#     try:
#         if scraping_process and scraping_process.poll() is None:
#             # Send pause command
#             scraping_process.stdin.write('p\n')
#             scraping_process.stdin.flush()
#             return {"message": "Pause signal sent successfully"}
#         else:
#             raise HTTPException(status_code=400, detail="No active scraping process found")

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to pause scraping: {str(e)}")

# @app.post("/resume_scraping")
# async def resume_scraping(current_user: dict = Depends(get_current_user)):
#     """Resume the scraping process"""
#     global scraping_process

#     if current_user.role != "admin":
#         raise HTTPException(status_code=403, detail="Admin access required")

#     try:
#         if scraping_process and scraping_process.poll() is None:
#             # Send resume command
#             scraping_process.stdin.write('r\n')
#             scraping_process.stdin.flush()
#             return {"message": "Resume signal sent successfully"}
#         else:
#             raise HTTPException(status_code=400, detail="No active scraping process found")

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to resume scraping: {str(e)}")


@app.get("/chat_sessions")
async def get_chat_sessions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all chat sessions for the current user"""
    sessions = db.query(ChatSession).filter(
        ChatSession.user_id == current_user.id
    ).order_by(ChatSession.updated_at.desc()).all()

    return [
        {
            "id": session.id,
            "session_id": session.session_id,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
            "message_count": len(session.messages)
        }
        for session in sessions
    ]

@app.get("/chat_sessions/{session_id}/messages")
async def get_session_messages(
    session_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all messages for a specific chat session"""
    session = db.query(ChatSession).filter(
        ChatSession.session_id == session_id,
        ChatSession.user_id == current_user.id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Chat session not found")

    messages = db.query(ChatMessage).filter(
        ChatMessage.session_id == session.id
    ).order_by(ChatMessage.created_at.asc()).all()

    formatted_messages = []
    for msg in messages:
        message_data = {
            "id": msg.id,
            "message": msg.message,
            "response": msg.response,
            "is_grounded": msg.is_grounded,
            "grounding_metadata": msg.grounding_metadata,
            "query_type": msg.query_type,
            "created_at": msg.created_at
        }

        # Add SQL-specific data if available
        if msg.sql_query:
            message_data["sql_query"] = msg.sql_query
        if msg.query_results:
            try:
                message_data["query_results"] = json.loads(msg.query_results)
            except Exception as e:
                logger.error(f"Error parsing query results JSON: {e}")
                message_data["query_results"] = None

        formatted_messages.append(message_data)

    return formatted_messages

@app.delete("/chat_sessions/{session_id}")
async def delete_chat_session(
    session_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a chat session and all its messages"""
    session = db.query(ChatSession).filter(
        ChatSession.session_id == session_id,
        ChatSession.user_id == current_user.id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Chat session not found")

    # Delete all messages in the session
    db.query(ChatMessage).filter(ChatMessage.session_id == session.id).delete()

    # Delete the session
    db.delete(session)
    db.commit()

    return {"message": "Chat session deleted successfully"}

# Database info endpoint
@app.get("/database/info")
async def get_database_info(
    current_user: User = Depends(get_current_user)
):
    """Get information about the partners8_data database"""
    schema_data = get_database_schema()

    if not schema_data:
        return {
            "available": False,
            "message": "Database not available. Please run data scraping first."
        }

    return {
        "available": True,
        "total_rows": schema_data["total_rows"],
        "columns": [
            {
                "name": col[1],
                "type": col[2],
                "nullable": bool(col[3]),
                "primary_key": bool(col[5])
            }
            for col in schema_data["columns"]
        ],
        "sample_queries": [
            "What are the top 10 most expensive cities?",
            "Show me cities in California with high rent prices",
            "Which states have the lowest income limits?",
            "Find cities where median rent is above $3000",
            "Compare rent prices between Texas and Florida"
        ]
    }

# Dashboard and statistics endpoints
@app.get("/dashboard/stats")
async def get_dashboard_stats(
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db)
):
    """Get dashboard statistics for admin users"""
    total_users = db.query(User).count()
    approved_users = db.query(User).filter(User.is_approved == True).count()
    pending_users = db.query(User).filter(User.is_approved == False).count()
    admin_users = db.query(User).filter(User.role == "admin").count()

    total_chat_sessions = db.query(ChatSession).count()
    total_messages = db.query(ChatMessage).count()
    grounded_messages = db.query(ChatMessage).filter(ChatMessage.is_grounded == True).count()
    data_queries = db.query(ChatMessage).filter(ChatMessage.query_type == "data_query").count()

    recent_scraping_logs = db.query(ScrapingLog).order_by(
        ScrapingLog.started_at.desc()
    ).limit(5).all()

    # Database info
    schema_data = get_database_schema()
    database_rows = schema_data["total_rows"] if schema_data else 0

    return {
        "users": {
            "total": total_users,
            "approved": approved_users,
            "pending": pending_users,
            "admins": admin_users
        },
        "chat": {
            "total_sessions": total_chat_sessions,
            "total_messages": total_messages,
            "grounded_messages": grounded_messages,
            "data_queries": data_queries,
            "grounding_percentage": round((grounded_messages / total_messages * 100) if total_messages > 0 else 0, 2)
        },
        "database": {
            "available": schema_data is not None,
            "total_rows": database_rows
        },
        "recent_scraping": [
            {
                "id": log.id,
                "status": log.status,
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "completed_at": log.completed_at.isoformat() if log.completed_at else None,
                "records_processed": log.records_processed,
                "current_step": log.current_step,
                "total_steps": log.total_steps,
                "step_name": log.step_name,
                "progress_percentage": log.progress_percentage
            }
            for log in recent_scraping_logs
        ]
    }

@app.get("/dashboard/user_stats")
async def get_user_dashboard_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get dashboard statistics for regular users"""
    user_sessions = db.query(ChatSession).filter(
        ChatSession.user_id == current_user.id
    ).count()

    user_messages = db.query(ChatMessage).join(ChatSession).filter(
        ChatSession.user_id == current_user.id
    ).count()

    user_grounded_messages = db.query(ChatMessage).join(ChatSession).filter(
        ChatSession.user_id == current_user.id,
        ChatMessage.is_grounded == True
    ).count()

    user_data_queries = db.query(ChatMessage).join(ChatSession).filter(
        ChatSession.user_id == current_user.id,
        ChatMessage.query_type == "data_query"
    ).count()

    recent_sessions = db.query(ChatSession).filter(
        ChatSession.user_id == current_user.id
    ).order_by(ChatSession.updated_at.desc()).limit(5).all()

    # Database info
    schema_data = get_database_schema()

    return {
        "chat": {
            "total_sessions": user_sessions,
            "total_messages": user_messages,
            "grounded_messages": user_grounded_messages,
            "data_queries": user_data_queries,
            "grounding_percentage": round((user_grounded_messages / user_messages * 100) if user_messages > 0 else 0, 2)
        },
        "database": {
            "available": schema_data is not None,
            "total_rows": schema_data["total_rows"] if schema_data else 0
        },
        "recent_sessions": [
            {
                "session_id": session.session_id,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "message_count": len(session.messages)
            }
            for session in recent_sessions
        ]
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db = SessionLocal()
        db.execute(text("SELECT 1")) # Use text() for literal SQL
        db.close()
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    try:
        # Test Google AI client
        client = get_genai_client()
        ai_status = "healthy" if client else "unhealthy: client initialization failed"
    except Exception as e:
        ai_status = f"unhealthy: {str(e)}"

    try:
        # Test data database
        schema_data = get_database_schema()
        data_db_status = "healthy" if schema_data else "unavailable: no data table found"
    except Exception as e:
        data_db_status = f"unhealthy: {str(e)}"

    return {
        "status": "healthy" if all("healthy" in status for status in [db_status, ai_status]) else "degraded",
        "timestamp": datetime.utcnow(),
        "services": {
            "database": db_status,
            "google_ai": ai_status,
            "data_database": data_db_status
        }
    }

# Test endpoints for development
@app.get("/test/ai")
async def test_ai_connection(
    current_user: User = Depends(get_current_admin_user)
):
    """Test Google AI connection"""
    try:
        result = await search_with_google_grounding("Hello, this is a test message")
        return {
            "success": True,
            "response": result["response"][:100] + "..." if len(result["response"]) > 100 else result["response"],
            "is_grounded": result["is_grounded"],
            "sources_count": len(result["sources"])
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/test/data_query")
async def test_data_query(
    query: str = "What are the top 5 most expensive cities?",
    current_user: User = Depends(get_current_admin_user)
):
    """Test data query functionality"""
    try:
        # Test SQL generation
        sql_result = await natural_language_to_sql(query)
        if not sql_result["success"]:
            return {"success": False, "error": f"SQL generation failed: {sql_result['error']}"}

        # Test SQL execution
        execution_result = await execute_sql_query(sql_result["sql_query"])
        if not execution_result["success"]:
            return {"success": False, "error": f"SQL execution failed: {execution_result['error']}"}

        # Test summary generation
        summary = await summarize_query_results(query, sql_result["sql_query"], execution_result["results"])

        return {
            "success": True,
            "query": query,
            "sql_query": sql_result["sql_query"],
            "results_count": execution_result["row_count"],
            "summary": summary[:200] + "..." if len(summary) > 200 else summary
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# API documentation endpoints
@app.get("/api/info")
async def get_api_info():
    """Get API information"""
    return {
        "title": "Partners8 Management System",
        "version": "2.0.0",
        "description": "A comprehensive system with user management, AI chat with integrated data query capabilities, and data scraping",
        "endpoints": {
            "authentication": ["/token", "/signup", "/verify-token"],
            "user_management": ["/users", "/approve_user/{user_id}", "/promote_to_admin/{user_id}"],
            "chat": ["/chat", "/chat_sessions", "/chat_sessions/{session_id}/messages"],
            "database": ["/database/info"],
            "scraping": ["/start_scraping", "/stop_scraping", "/scraping_status", "/scraping_logs"],
            "dashboard": ["/dashboard/stats", "/dashboard/user_stats"],
            "health": ["/health"],
            "testing": ["/test/ai", "/test/data_query"]
        },
        "features": [
            "User authentication and authorization",
            "Role-based access control (admin/user)",
            "Google AI integration with grounding search",
            "Intelligent query routing (data queries vs general chat)",
            "Natural language to SQL conversion for real estate data",
            "Chat sessions with message history and query results",
            "Background scraping tasks",
            "Dashboard statistics",
            "Health monitoring"
        ],
        "chat_capabilities": {
            "general_queries": "Answered using Google AI with grounding search",
            "data_queries": "Automatically converted to SQL and executed against real estate database",
            "fallback": "If data query fails, falls back to grounded search",
            "supported_data_types": [
                "Real estate prices and rents",
                "Income limits and demographics",
                "HUD Fair Market Rents",
                "Rent-to-value ratios",
                "Geographic data (cities, states, counties)"
            ]
        }
    }

frontend_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "partner8-frontend", "out")


@app.get("/dashboard")
async def serve_dashboard():
    return FileResponse(os.path.join(frontend_dir, "dashboard.html"))

@app.get("/dashboard/{path:path}")
async def serve_dashboard_subpaths(path: str):
    # This will serve files like /dashboard/chat, /dashboard/users, etc.
    # It will look for dashboard/chat.html, dashboard/users.html etc.
    # If not found, it will fall back to dashboard.html
    file_path = os.path.join(frontend_dir, "dashboard", f"{path}.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return FileResponse(os.path.join(frontend_dir, "dashboard.html"))

# Serve static files from the Next.js build
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info",reload=True)

