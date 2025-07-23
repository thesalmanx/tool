import streamlit as st
import sqlite3
import pandas as pd
import os
import requests
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import hashlib # For password hashing
import subprocess # To run background tasks
import time # To add delays for auto-refresh

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Partners 8 Real Estate Analytics",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
DATABASE_FILE = "partners8_data.db"
ROLES = ["user", "admin", "super_admin"]
STATUS_FILE = "scraping_status.json"
LOG_FILE = "scraping.log"
STOP_FILE = "stop_scraping.json" # New: File to signal stopping the pipeline


# --- Database Initialization and Management ---
def init_db():
    """Initializes the SQLite database, creating tables if they don't exist."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        # Create users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,\
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                status TEXT NOT NULL DEFAULT 'pending' -- pending, active, rejected
            )
        """)
        conn.commit()

        # Add a default super admin if none exists
        cursor.execute("SELECT * FROM users WHERE role='super_admin'")
        if not cursor.fetchone():
            username = os.getenv('SUPER_ADMIN_USERNAME', 'superadmin')
            password = os.getenv('SUPER_ADMIN_PASSWORD', 'superpass')
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            try:
                cursor.execute("INSERT INTO users (username, password_hash, role, status) VALUES (?, ?, ?, ?)",
                               (username, hashed_password, 'super_admin', 'active'))
                conn.commit()
                st.success(f"Default super admin '{username}' created. Password: '{password}'")
            except sqlite3.IntegrityError:
                # This is fine, means it already exists
                pass


# --- Authentication Functions ---
def hash_password(password):
    """Hashes a password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed_password):
    """Verifies a password against its hash."""
    return hash_password(password) == hashed_password

def signup_user(username, password):
    """Registers a new user with 'pending' status."""
    hashed_password = hash_password(password)
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (username, password_hash, role, status) VALUES (?, ?, ?, ?)",
                           (username, hashed_password, 'user', 'pending'))
            conn.commit()
        return True, "Signup successful! Your account is pending admin approval."
    except sqlite3.IntegrityError:
        return False, "Username already exists. Please choose a different one."
    except Exception as e:
        return False, f"Signup failed: {e}"

def login_user(username, password):
    """Authenticates a user."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE username=?", (username,))
        user_data = cursor.fetchone()
        if user_data:
            user_id, db_username, db_password_hash, role, status = user_data
            if verify_password(password, db_password_hash):
                if status == 'active':
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = db_username
                    st.session_state['user_role'] = role
                    st.session_state['user_id'] = user_id
                    return True, "Login successful!"
                elif status == 'pending':
                    return False, "Your account is pending admin approval."
                elif status == 'rejected':
                    return False, "Your account has been rejected by an admin."
            else:
                return False, "Incorrect password."
        else:
            return False, "Username not found."

def logout_user():
    """Logs out the current user."""
    for key in ['logged_in', 'username', 'user_role', 'user_id']:
        if key in st.session_state:
            del st.session_state[key]
    st.success("You have been logged out.")
    st.rerun()

# --- Role-based Access Control (RBAC) ---
def is_logged_in():
    return st.session_state.get('logged_in', False)

def get_current_user_role():
    return st.session_state.get('user_role', 'guest')

def has_role(required_role):
    current_role = get_current_user_role()
    if required_role == 'user':
        return current_role in ROLES
    elif required_role == 'admin':
        return current_role in ['admin', 'super_admin']
    elif required_role == 'super_admin':
        return current_role == 'super_admin'
    return False

# --- UI Components for Authentication ---
def show_login_page():
    st.title("Login / Signup")
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Login")
        login_username = st.text_input("Username", key="login_username")
        login_password = st.text_input("Password", type="password", key="login_password")
        if st.button("Login", type="primary", key="login_button"):
            if login_username and login_password:
                success, message = login_user(login_username, login_password)
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
            else:
                st.warning("Please enter both username and password.")

    with col2:
        st.subheader("Signup")
        signup_username = st.text_input("New Username", key="signup_username")
        signup_password = st.text_input("New Password", type="password", key="signup_password")
        signup_confirm_password = st.text_input("Confirm Password", type="password", key="signup_confirm_password")

        if st.button("Signup", key="signup_button"):
            if signup_username and signup_password and signup_confirm_password:
                if signup_password == signup_confirm_password:
                    if len(signup_password) < 6:
                        st.error("Password must be at least 6 characters long.")
                    else:
                        success, message = signup_user(signup_username, signup_password)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                else:
                    st.error("Passwords do not match.")
            else:
                st.warning("Please fill in all signup fields.")

    st.markdown("---")

# --- Admin Panel ---
def show_admin_panel():
    st.title("Admin Panel")
    st.markdown("---")

    if not has_role('admin'):
        st.warning("You do not have sufficient permissions to access the Admin Panel.")
        return

    admin_tabs = st.tabs(["User Management", "Data Management"])

    with admin_tabs[0]:
        manage_users()
    
    with admin_tabs[1]:
        manage_data_scraping()


def manage_users():
    """UI for managing user accounts."""
    st.subheader("Manage User Accounts")

    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role, status FROM users")
        users_data = cursor.fetchall()
        users_df = pd.DataFrame(users_data, columns=["ID", "Username", "Role", "Status"])

    if not users_df.empty:
        st.dataframe(users_df, use_container_width=True)

        st.markdown("---")
        st.subheader("Update User Status/Role")

        user_to_update = st.selectbox("Select User to Update", options=users_df['Username'].tolist(), key="user_select_admin")

        if user_to_update:
            selected_user_data = users_df[users_df['Username'] == user_to_update].iloc[0]
            current_user_role_of_selected = selected_user_data['Role']
            current_user_status_of_selected = selected_user_data['Status']

            new_status = st.selectbox(f"New Status for {user_to_update}", options=['pending', 'active', 'rejected'], 
                                      index=['pending', 'active', 'rejected'].index(current_user_status_of_selected),
                                      key=f"status_for_{user_to_update}")
            
            available_roles_for_selection = ROLES
            if get_current_user_role() == 'admin':
                if current_user_role_of_selected in ['admin', 'super_admin']:
                    available_roles_for_selection = [current_user_role_of_selected]
                else:
                    available_roles_for_selection = ["user", "admin"]

            new_role = st.selectbox(f"New Role for {user_to_update}", options=available_roles_for_selection, 
                                    index=available_roles_for_selection.index(current_user_role_of_selected),
                                    key=f"role_for_{user_to_update}")

            if st.button(f"Apply Changes to {user_to_update}", type="primary", key=f"apply_changes_{user_to_update}"):
                can_update = True
                error_message = ""

                if get_current_user_role() == 'admin' and current_user_role_of_selected == 'admin' and new_status == 'rejected':
                    can_update = False
                    error_message = "Admins cannot reject other admin accounts."
                
                if get_current_user_role() == 'admin':
                    if current_user_role_of_selected in ['admin', 'super_admin'] and new_role != current_user_role_of_selected:
                        can_update = False
                        error_message = "Admins cannot change the role of other admin or super admin accounts."
                    elif new_role == 'super_admin':
                        can_update = False
                        error_message = "Admins cannot promote users to super admin."
                
                if user_to_update == st.session_state['username'] and get_current_user_role() == 'super_admin':
                    if new_role != 'super_admin' or new_status != 'active':
                        can_update = False
                        error_message = "Super admin cannot change their own role or status."

                if can_update:
                    try:
                        with sqlite3.connect(DATABASE_FILE) as conn:
                            cursor = conn.cursor()
                            cursor.execute("UPDATE users SET status=?, role=? WHERE username=?",
                                           (new_status, new_role, user_to_update))
                            conn.commit()
                        st.success(f"Successfully updated {user_to_update}'s status to '{new_status}' and role to '{new_role}'.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error updating user: {e}")
                else:
                    st.error(error_message)
    else:
        st.info("No users registered yet.")

    if has_role('super_admin'):
        st.markdown("---")
        st.subheader("Delete User Account (Super Admin Only)")
        user_to_delete = st.selectbox("Select User to Delete", options=users_df['Username'].tolist(), key="user_select_delete")
        if user_to_delete and st.button(f"Delete {user_to_delete}", help="This action cannot be undone.", type="secondary", key=f"delete_user_{user_to_delete}"):
            if user_to_delete == st.session_state['username']:
                st.error("You cannot delete your own account!")
            elif users_df[users_df['Username'] == user_to_delete]['Role'].iloc[0] == 'super_admin' and get_current_user_role() != 'super_admin':
                 st.error("Only a Super Admin can delete another Super Admin.")
            else:
                try:
                    with sqlite3.connect(DATABASE_FILE) as conn:
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM users WHERE username=?", (user_to_delete,))
                        conn.commit()
                    st.success(f"User '{user_to_delete}' deleted successfully.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error deleting user: {e}")

# --- Data Scraping Management ---
def get_scraping_status():
    """Reads the scraping status from the JSON file."""
    if not os.path.exists(STATUS_FILE):
        return {"status": "idle", "last_success_date": None, "progress": 0} # New: Default progress to 0
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"status": "unknown", "last_success_date": None, "error": "Status file is corrupted or missing.", "progress": 0}

def read_log_file():
    """Reads the content of the log file."""
    if not os.path.exists(LOG_FILE):
        return "Log file not found. A new one will be created on the next run."
    with open(LOG_FILE, 'r') as f:
        return f.read()

def manage_data_scraping():
    """UI for managing the data scraping task."""
    st.subheader("Data Scraping Management")

    status_data = get_scraping_status()
    status = status_data.get('status', 'idle')
    last_success = status_data.get('last_success_date')
    progress = status_data.get('progress', 0) # New: Get progress

    if last_success:
        last_success_dt = datetime.fromisoformat(last_success)
        st.info(f"**Last Successful Scrape:** {last_success_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("**Last Successful Scrape:** Never")

    st.write(f"**Current Status:** `{status.capitalize()}`")

    if status == 'running':
        st.warning("A scraping task is currently in progress. Please wait for it to complete.")
        message = status_data.get('message', 'Running...')
        st.write(f"**Current Step:** {message}")
        st.progress(progress / 100.0, text=f"Progress: {progress:.1f}%") # New: Display progress bar
    elif status == 'failed':
        st.error(f"The last scraping task failed. Please check the logs for details.")
        error_details = status_data.get('error', 'No error details available.')
        st.write(f"**Error:** {error_details}")
    elif status == 'stopped': # New: Handle stopped status
        st.info("The last scraping task was stopped by the user.")
        message = status_data.get('message', 'Stopped.')
        st.write(f"**Status Message:** {message}")


    col_buttons = st.columns(2)
    with col_buttons[0]:
        # The button to start the scraping task
        if st.button("Start New Scrape", disabled=(status == 'running'), type="primary", key="start_scrape_button"):
            try:
                st.toast("🚀 Starting background scraping task...")
                # Ensure the stop file is removed before starting a new scrape
                if os.path.exists(STOP_FILE):
                    os.remove(STOP_FILE)
                # Run main.py as a separate process
                subprocess.Popen(["python3", "main.py"])
                time.sleep(2) # Give it a moment to start and update status
                st.rerun()
            except Exception as e:
                st.error(f"Failed to start scraping process: {e}")
    
    with col_buttons[1]:
        # New: Stop button for Super Admin
        if has_role('super_admin'):
            if st.button("Stop Scrape", disabled=(status != 'running'), type="secondary", key="stop_scrape_button"):
                try:
                    # Create the stop signal file
                    with open(STOP_FILE, 'w') as f:
                        json.dump({"stop": True, "requested_at": datetime.now().isoformat()}, f)
                    st.toast("🛑 Stop signal sent. The pipeline will halt shortly.")
                    time.sleep(1) # Give it a moment to register
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to send stop signal: {e}")

    # Live Log Viewer
    with st.expander("Live Log Viewer", expanded=(status == 'running')):
        log_placeholder = st.empty()
        log_content = read_log_file()
        log_placeholder.code(log_content, language="log")

    # Auto-refresh logic when a task is running
    if status == 'running':
        time.sleep(5) # Refresh every 5 seconds
        st.rerun()

# --- Main Application Logic (from original stream.py, adapted) ---
class StreamlitSQLQuery:
    def __init__(self):
        """Initialize the Streamlit SQL Query tool"""
        self.api_key = os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            st.error("⚠️ GEMINI_API_KEY not found in environment variables")
            st.stop()
        
        self.database_schema = self.get_database_schema()
    
    def get_database_schema(self):
        """Get the database schema information"""
        if not os.path.exists(DATABASE_FILE):
            st.error(f"❌ Database file not found: {DATABASE_FILE}")
            st.info("Please run the data scraping task from the Admin Panel first to create the database.")
            st.stop()
        
        try:
            with sqlite3.connect(DATABASE_FILE) as conn:
                cursor = conn.cursor()
                # Check if table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='partners8_data'")
                if cursor.fetchone() is None:
                    st.error("❌ The 'partners8_data' table does not exist in the database.")
                    st.info("Please run the data scraping task from the Admin Panel to populate the data.")
                    st.stop()

                cursor.execute("PRAGMA table_info(partners8_data)")
                columns = cursor.fetchall()
                cursor.execute("SELECT COUNT(*) FROM partners8_data")
                total_rows = cursor.fetchone()[0]
                
                return {
                    'columns': columns,
                    'total_rows': total_rows
                }
        except Exception as e:
            st.error(f"❌ Failed to load database schema: {e}")
            st.stop()
    
    def call_gemini_api(self, prompt, model="gemini-2.0-flash-exp"): # New: Changed model to gemini-2.0-flash-exp
        """Call Gemini API directly using REST"""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        
        headers = {'Content-Type': 'application/json'}
        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 4096,
                "candidateCount": 1
            }
        }
        
        try:
            response = requests.post(
                f"{url}?key={self.api_key}",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'candidates' in result and len(result['candidates']) > 0:
                    return result['candidates'][0]['content']['parts'][0]['text']
                else:
                    raise Exception("No response generated")
            else:
                raise Exception(f"API call failed with status {response.status_code}: {response.text}")
                
        except Exception as e:
            raise Exception(f"Error calling Gemini API: {e}")
    
    def create_schema_prompt(self):
        """Create a detailed schema prompt for Gemini"""
        column_descriptions = {
            'id': 'Primary key, auto-increment',
            'Region': 'Zillow Region ID',
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
            'ZH Ratio': 'HUD 4-bedroom rent to Zillow home value ratio', # Corrected column name for prompt
            'NH Ratio': 'HUD 4-bedroom rent to NAR home value ratio', # Corrected column name for prompt
            'created_at': 'Record creation timestamp',
            'updated_at': 'Record update timestamp'
        }
        
        schema_text = "DATABASE SCHEMA FOR PARTNERS 8 REAL ESTATE DATA:\n\n"
        schema_text += "Table: partners8_data\n\nColumns:\n"
        
        for col in self.database_schema['columns']:
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
        
        return schema_text
    
    def natural_language_to_sql(self, user_question):
        """Convert natural language question to SQL query using Gemini"""
        schema_prompt = self.create_schema_prompt()
        
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
6. Make sure column names match exactly (case-sensitive, and use double quotes for columns with spaces like "ZH Ratio")
7. Start directly with SELECT

Generate a complete, executable SQL query:
"""
        
        try:
            response_text = self.call_gemini_api(prompt)
            sql_query = self.clean_sql_query(response_text)
            return sql_query
            
        except Exception as e:
            st.error(f"❌ Error generating SQL query: {e}")
            return None
    
    def get_corrected_sql_query(self, user_question, sql_query, error_message):
        """Ask Gemini to correct the SQL query"""
        schema_prompt = self.create_schema_prompt()
        
        prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"

The following SQL query failed:
{sql_query}

ERROR MESSAGE:
{error_message}

Please correct the SQL query.
- Return ONLY the corrected SQL query.
- Do not add any explanations or markdown.
- Ensure the query is valid SQLite.
- Make sure to use the correct column names from the schema (e.g., "ZH Ratio").
- If a column name has a space, enclose it in double quotes, like "ZH Ratio".
"""
        
        try:
            response_text = self.call_gemini_api(prompt)
            corrected_query = self.clean_sql_query(response_text)
            return corrected_query
        except Exception as e:
            st.error(f"❌ Error getting corrected SQL query: {e}")
            return None

    def clean_sql_query(self, sql_query):
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

    def execute_sql_query(self, user_question, sql_query):
        """Execute the SQL query and return results"""
        try:
            with sqlite3.connect(DATABASE_FILE) as conn:
                df = pd.read_sql_query(sql_query, conn)
                return df
        except Exception as e:
            st.warning(f"Initial query failed: {e}. Attempting to correct...")
            
            corrected_query = self.get_corrected_sql_query(user_question, sql_query, str(e))
            
            if corrected_query:
                st.info("Corrected SQL Query:")
                st.code(corrected_query, language="sql")
                try:
                    with sqlite3.connect(DATABASE_FILE) as conn:
                        df = pd.read_sql_query(corrected_query, conn)
                        return df
                except Exception as e2:
                    st.error(f"❌ Error executing corrected SQL query: {e2}")
                    return None
            else:
                st.error("Failed to get a corrected query.")
                return None
    
    def summarize_results(self, user_question, sql_query, results_df):
        """Use Gemini to summarize the query results"""
        if len(results_df) == 0:
            return "No results found for your query."
        
        display_df = results_df.head(10)
        results_text = display_df.to_string(index=False, max_cols=10)
        
        if len(results_df) > 10:
            results_text += f"\n... and {len(results_df) - 10} more rows"
        
        prompt = f"""
ORIGINAL QUESTION: "{user_question}"
SQL QUERY: {sql_query}
RESULTS: {results_text}

Provide a clear, concise summary of these results in 2-3 sentences. Focus on key insights and patterns.
"""
        
        try:
            response_text = self.call_gemini_api(prompt)
            return response_text.strip()
        except Exception as e:
            return f"Results found but unable to generate summary: {e}"

def create_visualizations(df, query_type):
    """Create appropriate visualizations based on the data"""
    if df is None or len(df) == 0:
        return None
    
    charts = []
    
    numeric_columns = df.select_dtypes(include=['number']).columns
    categorical_columns = df.select_dtypes(include=['object']).columns
    
    if len(numeric_columns) > 0 and len(categorical_columns) > 0:
        if 'State' in df.columns and len(numeric_columns) > 0:
            numeric_col = numeric_columns[0]
            if len(df) <= 20:
                fig = px.bar(df, x='State', y=numeric_col, 
                           title=f'{numeric_col} by State',
                           color=numeric_col,
                           color_continuous_scale='viridis')
                charts.append(fig)
    
    if len(numeric_columns) >= 2:
        fig = px.scatter(df, x=numeric_columns[0], y=numeric_columns[1],
                        title=f'{numeric_columns[0]} vs {numeric_columns[1]}',
                        hover_data=df.columns[:5])
        charts.append(fig)
    
    if len(numeric_columns) == 1:
        fig = px.histogram(df, x=numeric_columns[0], nbins=30,
                          title=f'Distribution of {numeric_columns[0]}')
        charts.append(fig)
    
    return charts

def main_app_content():
    """Main Streamlit application content for logged-in users."""
    st.title("🏠 Partners 8 Real Estate Analytics")
    st.markdown("Ask questions about real estate data in plain English!")
    
    try:
        query_tool = StreamlitSQLQuery()
    except Exception as e:
        # This will catch the st.stop() from the init and prevent crashing
        return
    
    with st.sidebar:
        st.header("📊 Database Info")
        st.metric("Total Records", f"{query_tool.database_schema['total_rows']:,}")
        
        st.header("🔍 Example Questions")
        examples = [
            "What are the top 10 most expensive cities?",
            "Show me cities in California with high rent-to-value ratios",
            "Which states have the lowest income limits?",
            "Find cities where median rent is above $3000",
            "Compare rent prices between Texas and Florida",
            "Show me the most affordable cities for families"
        ]
        
        # Initialize user_question in session_state if not present
        if 'user_question' not in st.session_state:
            st.session_state.user_question = ''
            
        for example in examples:
            if st.button(f"📝 {example}", key=f"example_{example}", use_container_width=True):
                st.session_state.user_question = example
                st.rerun()
        
        st.header("📋 Available Data")
        st.info("""
        • Zillow rent and home values
        • HUD Fair Market Rents
        • Income limits by area
        • Rent-to-value ratios
        • City, county, and state data
        """)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        user_question = st.text_input(
            "Your Question:",
            value=st.session_state.user_question,
            key="user_question_input",
            placeholder="e.g., What are the most expensive cities in California?",
            help="Ask any question about the real estate data in natural language"
        )
    
    with col2:
        st.write("")
        analyze_button = st.button("🔍 Analyze", type="primary", use_container_width=True, key="analyze_button")
    
    if analyze_button and user_question:
        with st.spinner("🤔 Processing your question..."):
            st.subheader("📝 Generated SQL Query")
            sql_query = query_tool.natural_language_to_sql(user_question)
            
            if sql_query:
                st.code(sql_query, language="sql")
                st.subheader("📊 Query Results")
                results_df = query_tool.execute_sql_query(user_question, sql_query)
                
                if results_df is not None and len(results_df) > 0:
                    st.dataframe(results_df, use_container_width=True, height=400)
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name=f"partners8_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="download_csv_button"
                    )
                    
                    st.subheader("💡 AI Summary")
                    with st.spinner("Generating insights..."):
                        summary = query_tool.summarize_results(user_question, sql_query, results_df)
                        st.info(summary)
                    
                    st.subheader("📈 Visualizations")
                    charts = create_visualizations(results_df, "general")
                    
                    if charts:
                        for chart in charts:
                            st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info("No suitable visualizations could be generated for this data.")
                        
                elif results_df is not None and len(results_df) == 0:
                    st.warning("No results found for your query.")
                else:
                    st.error("Failed to execute the query. Please try rephrasing your question.")
            else:
                st.error("Failed to generate SQL query. Please try rephrasing your question.")

# --- Main Application Flow ---
def main():
    """Main Streamlit application entry point."""
    init_db()

    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['user_role'] = 'guest'
        st.session_state['user_id'] = None
    
    st.markdown("""
        <style>
        .stButton>button { border-radius: 20px; padding: 10px 20px; font-size: 16px; transition: all 0.2s ease-in-out; }
        .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
        .stTextInput>div>div>input { border-radius: 10px; padding: 10px; }
        .stSelectbox>div>div>div { border-radius: 10px; padding: 5px; }
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p { font-size: 1.2rem; }
        .stAlert { border-radius: 10px; }
        </style>
    """, unsafe_allow_html=True)

    if not is_logged_in():
        show_login_page()
    else:
        st.sidebar.header(f"Welcome, {st.session_state['username']}!")
        st.sidebar.write(f"Role: **{st.session_state['user_role'].replace('_', ' ').title()}**")

        app_tabs = ["Analytics"]
        if has_role('admin'):
            app_tabs.append("Admin Panel")
        
        # FIX: Added a unique key to the navigation radio button
        selected_tab = st.sidebar.radio("Navigation", app_tabs, key="nav_radio")

        if selected_tab == "Analytics":
            main_app_content()
        elif selected_tab == "Admin Panel":
            show_admin_panel()
        
        st.sidebar.markdown("---")
        if st.sidebar.button("Logout", type="secondary", use_container_width=True, key="logout_button"):
            logout_user()

if __name__ == "__main__":
    main()

