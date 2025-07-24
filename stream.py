import streamlit as st
import sqlite3
import pandas as pd
import os
import requests
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import hashlib # For password hashing
import subprocess # To run background tasks
import time # To add delays for auto-refresh
import uuid # For session tokens

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
SESSION_FILE = "user_sessions.json" # For persistent sessions


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


# --- Session Management Functions ---
def create_session_token():
    """Create a unique session token."""
    return str(uuid.uuid4())

def save_session(username, user_role, user_id):
    """Save session to file and return session token."""
    session_token = create_session_token()
    session_data = {
        "username": username,
        "user_role": user_role,
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(hours=24)).isoformat()  # 24 hour expiry
    }

    # Load existing sessions
    sessions = {}
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, 'r') as f:
                sessions = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            sessions = {}

    # Add new session
    sessions[session_token] = session_data

    # Clean up expired sessions
    current_time = datetime.now()
    sessions = {token: data for token, data in sessions.items()
                if datetime.fromisoformat(data['expires_at']) > current_time}

    # Save sessions
    with open(SESSION_FILE, 'w') as f:
        json.dump(sessions, f, indent=4)

    return session_token

def load_session(session_token):
    """Load session data from token."""
    if not session_token or not os.path.exists(SESSION_FILE):
        return None

    try:
        with open(SESSION_FILE, 'r') as f:
            sessions = json.load(f)

        if session_token not in sessions:
            return None

        session_data = sessions[session_token]

        # Check if session is expired
        if datetime.fromisoformat(session_data['expires_at']) <= datetime.now():
            # Remove expired session
            del sessions[session_token]
            with open(SESSION_FILE, 'w') as f:
                json.dump(sessions, f, indent=4)
            return None

        return session_data

    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        return None

def delete_session(session_token):
    """Delete a session token."""
    if not session_token or not os.path.exists(SESSION_FILE):
        return

    try:
        with open(SESSION_FILE, 'r') as f:
            sessions = json.load(f)

        if session_token in sessions:
            del sessions[session_token]
            with open(SESSION_FILE, 'w') as f:
                json.dump(sessions, f, indent=4)

    except (json.JSONDecodeError, FileNotFoundError):
        pass

def get_browser_session_id():
    """Get a unique browser session identifier."""
    # Use Streamlit's session ID if available
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        ctx = get_script_run_ctx()
        if ctx:
            return ctx.session_id
    except:
        pass

    # Fallback: create a session ID based on timestamp and store in session state
    if 'browser_session_id' not in st.session_state:
        st.session_state['browser_session_id'] = str(uuid.uuid4())

    return st.session_state['browser_session_id']

def get_persistent_session_file():
    """Get the path to the persistent session file for this browser session."""
    browser_id = get_browser_session_id()
    return f"session_{browser_id}.json"

def save_persistent_session(username, user_role, user_id):
    """Save session data to a persistent file."""
    session_file = get_persistent_session_file()
    session_data = {
        "username": username,
        "user_role": user_role,
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(hours=24)).isoformat()
    }

    try:
        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=4)
        return True
    except Exception as e:
        st.error(f"Failed to save session: {e}")
        return False

def load_persistent_session():
    """Load session data from persistent file."""
    session_file = get_persistent_session_file()

    if not os.path.exists(session_file):
        return None

    try:
        with open(session_file, 'r') as f:
            session_data = json.load(f)

        # Check if session is expired
        if datetime.fromisoformat(session_data['expires_at']) <= datetime.now():
            os.remove(session_file)
            return None

        return session_data
    except Exception as e:
        # Remove corrupted session file
        try:
            os.remove(session_file)
        except:
            pass
        return None

def clear_persistent_session():
    """Clear the persistent session file."""
    session_file = get_persistent_session_file()
    try:
        if os.path.exists(session_file):
            os.remove(session_file)
    except Exception:
        pass

def cleanup_old_sessions():
    """Clean up expired session files."""
    try:
        current_time = datetime.now()
        for filename in os.listdir('.'):
            if filename.startswith('session_') and filename.endswith('.json'):
                try:
                    with open(filename, 'r') as f:
                        session_data = json.load(f)

                    # Check if session is expired
                    if datetime.fromisoformat(session_data['expires_at']) <= current_time:
                        os.remove(filename)
                except Exception:
                    # Remove corrupted session files
                    try:
                        os.remove(filename)
                    except:
                        pass
    except Exception:
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
                    # Save persistent session
                    save_persistent_session(db_username, role, user_id)

                    # Set session state
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
    # Clear persistent session
    clear_persistent_session()

    # Clear session state
    for key in ['logged_in', 'username', 'user_role', 'user_id', 'browser_session_id']:
        if key in st.session_state:
            del st.session_state[key]

    st.success("You have been logged out.")
    st.rerun()

def restore_session_if_available():
    """Restore session from persistent storage if available."""
    session_data = load_persistent_session()

    if session_data:
        # Restore session state
        st.session_state['logged_in'] = True
        st.session_state['username'] = session_data['username']
        st.session_state['user_role'] = session_data['user_role']
        st.session_state['user_id'] = session_data['user_id']
        return True

    return False

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
            status_data = json.load(f)

        # Check if process is actually running by verifying PID
        if status_data.get('status') == 'running':
            if os.path.exists("scraping_pid.json"):
                try:
                    with open("scraping_pid.json", "r") as f:
                        pid_data = json.load(f)
                        stored_pid = pid_data.get("pid")

                    if stored_pid:
                        try:
                            import psutil
                            proc = psutil.Process(stored_pid)
                            if not proc.is_running():
                                # Process is not running, update status
                                status_data['status'] = 'failed'
                                status_data['error'] = 'Process terminated unexpectedly'
                                with open(STATUS_FILE, 'w') as f:
                                    json.dump(status_data, f, indent=4)
                        except (psutil.NoSuchProcess, ImportError):
                            # Process doesn't exist, update status
                            status_data['status'] = 'failed'
                            status_data['error'] = 'Process not found'
                            with open(STATUS_FILE, 'w') as f:
                                json.dump(status_data, f, indent=4)
                except (FileNotFoundError, json.JSONDecodeError):
                    pass

        return status_data
    except (json.JSONDecodeError, FileNotFoundError):
        return {"status": "unknown", "last_success_date": None, "error": "Status file is corrupted or missing.", "progress": 0}

def read_log_file():
    """Reads the content of the log file."""
    if not os.path.exists(LOG_FILE):
        return "Log file not found. A new one will be created on the next run."
    with open(LOG_FILE, 'r') as f:
        return f.read()

def can_user_stop_scraping():
    """Check if the current user can stop the running scraping process."""
    current_user = st.session_state.get('username')
    current_role = st.session_state.get('user_role')

    # Super admins can always stop any scraping
    if current_role == 'super_admin':
        return True, "Super admin privileges"

    # Check if the current user started the scraping
    if os.path.exists("scraping_pid.json"):
        try:
            with open("scraping_pid.json", "r") as f:
                pid_data = json.load(f)
                started_by_user = pid_data.get("started_by_user")

            if started_by_user == current_user:
                return True, f"Process started by you ({current_user})"
            else:
                return False, f"Process started by {started_by_user}. Only they or a super admin can stop it."

        except (FileNotFoundError, json.JSONDecodeError):
            # If we can't read the file, allow admins to stop
            if current_role == 'admin':
                return True, "Unable to verify starter, allowing admin access"

    return False, "You don't have permission to stop this process"

def manage_data_scraping():
    """UI for managing the data scraping task."""
    st.subheader("Data Scraping Management")

    # Add a note about session persistence
    
    status_data = get_scraping_status()
    status = status_data.get('status', 'idle')
    last_success = status_data.get('last_success_date')
    progress = status_data.get('progress', 0) # New: Get progress

    if last_success:
        last_success_dt = datetime.fromisoformat(last_success)
        st.info(f"**Last Successful Scrape:** {last_success_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("**Last Successful Scrape:** Never")

    # Show current status with timestamp
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write(f"**Current Status:** `{status.capitalize()}`")
    with col2:
        current_time = datetime.now().strftime("%H:%M:%S")
        st.write(f"**Last Updated:** {current_time}")

        # Show auto-refresh indicator if enabled
        if status == 'running' and st.session_state.get('auto_refresh_logs', True):
            st.write("🔄 *Auto-refreshing*")

    # Show process information if running
    if status == 'running' and os.path.exists("scraping_pid.json"):
        try:
            with open("scraping_pid.json", "r") as f:
                pid_data = json.load(f)
                process_pid = pid_data.get("pid")
                started_at = pid_data.get("started_at")
                started_by_user = pid_data.get("started_by_user")
                started_by_role = pid_data.get("started_by_role")

            # Create info columns
            col1, col2 = st.columns(2)

            with col1:
                if process_pid:
                    st.info(f"🔧 **Scraping ID:** {process_pid}")
                if started_at:
                    start_time = datetime.fromisoformat(started_at)
                    st.info(f"⏰ **Started:** {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

            with col2:
                if started_by_user:
                    st.info(f"👤 **Started by:** {started_by_user}")
                if started_by_role:
                    st.info(f"🎭 **Role:** {started_by_role.replace('_', ' ').title()}")

        except (FileNotFoundError, json.JSONDecodeError):
            pass

    if status == 'running':
        message = status_data.get('message', 'Running...')

        # Check if it's in stopping state
        if 'stopping' in message.lower() or 'stop' in message.lower():
            st.warning("⏹️ **Stopping scraping task...** Please wait for the process to halt gracefully.")
            st.info("The pipeline will stop at the next safe checkpoint.")
        else:
            st.success("✅ **Scraping is running**")

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

    # CSV Download Section
    st.markdown("---")
    st.subheader("📥 Download Final Dataset")

    # Check if final CSV file exists
    final_csv_path = "partners8_final_data.csv"
    if os.path.exists(final_csv_path):
        try:
            # Read the CSV file
            with open(final_csv_path, 'r', encoding='utf-8') as f:
                csv_data = f.read()

            # Get file info
            file_size = os.path.getsize(final_csv_path)
            file_size_mb = file_size / (1024 * 1024)

            # Count rows (subtract 1 for header)
            row_count = len(csv_data.split('\n')) - 1

            st.info(f"**Dataset Info:** {row_count:,} records | {file_size_mb:.2f} MB")

            # Download button
            st.download_button(
                label="📥 Download Complete Dataset (CSV)",
                data=csv_data,
                file_name=f"partners8_complete_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_complete_csv_button",
                help="Download the complete scraped real estate dataset",
                use_container_width=True
            )

        except Exception as e:
            st.error(f"Error reading CSV file: {e}")
    else:
        st.warning("No final dataset available. Please run a successful scrape first.")

    col_buttons = st.columns(2)
    with col_buttons[0]:
        # The button to start the scraping task
        if st.button("Start New Scrape", disabled=(status == 'running'), type="primary", key="start_scrape_button"):
            try:
                st.toast("🚀 Starting background scraping task...")
                # Ensure the stop file is removed before starting a new scrape
                if os.path.exists(STOP_FILE):
                    os.remove(STOP_FILE)

                # Run main.py as a completely separate background process
                # Using shell=False and detaching from parent process
                process = subprocess.Popen(
                    ["python", "main.py"],
                    stdout=subprocess.DEVNULL,  # Don't capture stdout
                    stderr=subprocess.DEVNULL,  # Don't capture stderr
                    stdin=subprocess.DEVNULL,   # Don't provide stdin
                    start_new_session=True,     # Start in new session (Unix)
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0  # Windows compatibility
                )

                # Store the process ID and user info for later termination
                pid_data = {
                    "pid": process.pid,
                    "started_at": datetime.now().isoformat(),
                    "started_by_user": st.session_state.get('username'),
                    "started_by_role": st.session_state.get('user_role')
                }
                with open("scraping_pid.json", "w") as f:
                    json.dump(pid_data, f)

                st.success(f"✅ Background scraping started! Process ID: {process.pid}")
                st.info("💡 The scraping is now running independently. You can close this browser tab and the scraping will continue.")

                time.sleep(1) # Brief pause for UI update
                st.rerun()
            except Exception as e:
                st.error(f"Failed to start scraping process: {e}")

    with col_buttons[1]:
        # Stop button with permission checking
        can_stop, stop_reason = can_user_stop_scraping()

        if can_stop:
            if st.button("🛑 Stop Scrape", disabled=(status != 'running'), type="secondary", key="stop_scrape_button", help="Immediately kill the running scraping process"):
                try:
                    # Create the stop signal file
                    with open(STOP_FILE, 'w') as f:
                        json.dump({
                            "stop": True,
                            "requested_at": datetime.now().isoformat(),
                            "requested_by": st.session_state.get('username'),
                            "reason": stop_reason
                        }, f)

                    # Try to kill the process immediately using multiple methods
                    killed_processes = 0

                    # Method 1: Try to kill using stored PID first (most targeted)
                    try:
                        if os.path.exists("scraping_pid.json"):
                            with open("scraping_pid.json", "r") as f:
                                pid_data = json.load(f)
                                stored_pid = pid_data.get("pid")

                            if stored_pid:
                                import psutil
                                try:
                                    proc = psutil.Process(stored_pid)
                                    if proc.is_running():
                                        proc.terminate()  # Try graceful termination first
                                        try:
                                            proc.wait(timeout=2)  # Wait up to 2 seconds
                                        except psutil.TimeoutExpired:
                                            proc.kill()  # Force kill if graceful termination fails
                                        killed_processes += 1
                                        print(f"Killed stored scraping process PID: {stored_pid}")
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass

                            # Clean up the PID file
                            os.remove("scraping_pid.json")
                    except (FileNotFoundError, json.JSONDecodeError, ImportError):
                        pass

                    # Method 2: Use psutil to find and kill any remaining Python processes running main.py
                    try:
                        import psutil
                        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                            try:
                                if proc.info['name'] == 'python' and proc.info['cmdline']:
                                    cmdline = ' '.join(proc.info['cmdline'])
                                    if 'main.py' in cmdline:
                                        proc.terminate()  # Try graceful termination first
                                        try:
                                            proc.wait(timeout=2)  # Wait up to 2 seconds
                                        except psutil.TimeoutExpired:
                                            proc.kill()  # Force kill if graceful termination fails
                                        killed_processes += 1
                                        print(f"Killed scraping process PID: {proc.info['pid']}")
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                pass

                    except ImportError:
                        pass

                    # Method 3: Use subprocess as backup to kill any remaining processes
                    try:
                        # Kill any python processes with main.py in command line (Linux/Mac)
                        result = subprocess.run(['pkill', '-f', 'python.*main.py'],
                                              capture_output=True, text=True, timeout=5)
                        if result.returncode == 0:
                            killed_processes += 1
                    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
                        pass

                    # Provide feedback based on results
                    if killed_processes > 0:
                        st.success(f"🛑 **Process terminated immediately!** Killed {killed_processes} scraping process(es).")
                        st.info("✅ The scraping has been stopped forcefully.")
                    else:
                        st.warning("🛑 **Stop signal sent!** No active scraping processes found to kill.")
                        st.info("💡 The process may have already finished or stopped.")

                    # Update status to stopped
                    status_data = {"status": "stopped", "message": "Process stopped by user", "updated_at": datetime.now().isoformat()}
                    with open(STATUS_FILE, 'w') as f:
                        json.dump(status_data, f, indent=4)

                    time.sleep(1) # Brief pause for UI update
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to stop process: {e}")
        else:
            # Show why the user can't stop the scraping
            if status == 'running':
                st.warning(f"🔒 **Cannot stop scraping:** {stop_reason}")

                # Show additional info about permissions
                current_role = st.session_state.get('user_role')
                if current_role == 'admin':
                    st.info("� **Admin Note:** You can only stop scraping processes that you started, or ask a Super Admin to stop it.")
                elif current_role == 'user':
                    st.info("💡 **User Note:** Only Admins and Super Admins can manage scraping processes.")
            else:
                st.info("🔒 Only users who started the scraping (or Super Admins) can stop it.")

    # Live Log Viewer with Auto-refresh
    with st.expander("Live Log Viewer", expanded=(status == 'running')):
        if status == 'running':
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("🔄 Refresh Logs", key="refresh_logs_button", use_container_width=True):
                    st.rerun()

            with col2:
                auto_refresh_logs = st.checkbox("🔄 Auto-refresh logs", value=True, key="auto_refresh_logs",
                                              help="Automatically refresh logs every 4 seconds")

        log_placeholder = st.empty()

        def update_log_display():
            log_content = read_log_file()
            # Show last 50 lines of log to avoid UI overload
            log_lines = log_content.split('\n')
            if len(log_lines) > 50:
                log_content = '\n'.join(log_lines[-50:])
                log_content = "... (showing last 50 lines) ...\n" + log_content

            log_placeholder.code(log_content, language="log")

        # Initial log display
        update_log_display()

        # Auto-refresh logs if enabled and scraping is running
        if status == 'running' and auto_refresh_logs:
            # Initialize auto-refresh timer
            if 'auto_refresh_start_time' not in st.session_state:
                st.session_state.auto_refresh_start_time = time.time()

            elapsed_time = time.time() - st.session_state.auto_refresh_start_time

            # Auto-refresh every 4 seconds
            if elapsed_time >= 4:
                st.session_state.auto_refresh_start_time = time.time()
                st.rerun()
            else:
                remaining_time = 4 - int(elapsed_time)
                progress_percentage = (elapsed_time / 4.0) * 100

                

    # Manual refresh section
    if status == 'running':
        st.markdown("---")
        st.subheader("🔄 Status Updates")

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("🔄 Refresh Status", key="manual_refresh_button", type="primary", use_container_width=True):
                st.rerun()

        with col2:
            st.info("💡 **Tip:** Enable auto-refresh in the log viewer above for automatic updates")



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
        
        schema_text += f"\nDATABASE STATISTICS:\n"
        schema_text += f"- Total records: {self.database_schema['total_rows']:,}\n"
        schema_text += f"- Contains real estate data for US cities\n\n"

        schema_text += "IMPORTANT NOTES:\n"
        schema_text += "1. Use SQLite syntax only\n"
        schema_text += "2. All monetary values are in USD (rent/value prices)\n"
        schema_text += "3. State codes are 2-letter abbreviations (CA, TX, NY, etc.)\n"
        schema_text += "4. NULL values may exist - use IS NULL/IS NOT NULL appropriately\n"
        schema_text += "5. Ratios are decimal values (0.01 = 1%, 0.1 = 10%)\n"
        schema_text += "6. Only query the 'partners8_data' table\n"
        schema_text += "7. For columns with spaces like 'ZH Ratio', use double quotes: \"ZH Ratio\"\n"
        schema_text += "8. Use LIMIT for large result sets (default 100)\n"
        schema_text += "9. This database contains ONLY real estate pricing data - no demographics, crime, schools, etc.\n"
        schema_text += "10. Available data: rent prices, home values, income limits, rent-to-value ratios, city/state info\n"
        
        return schema_text
    
    def natural_language_to_sql(self, user_question):
        """Convert natural language question to SQL query using Gemini"""
        schema_prompt = self.create_schema_prompt()

        prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"

You are an expert SQL query generator for real estate data analysis. Convert this natural language question into a valid SQLite SQL query.

CRITICAL REQUIREMENTS:
1. Generate ONLY the SQL query, no explanations, comments, or markdown formatting
2. Use proper SQLite syntax with correct column names from the schema above
3. For columns with spaces (like "ZH Ratio", "NH Ratio"), use double quotes: "ZH Ratio"
4. Handle NULL values appropriately using IS NULL or IS NOT NULL
5. Use LIMIT clause for large result sets (default LIMIT 100 unless user specifies otherwise)
6. Use proper ORDER BY for meaningful sorting (e.g., highest to lowest values)
7. For price/value comparisons, use appropriate numeric operators (>, <, >=, <=, BETWEEN)
8. For text searches, use LIKE with wildcards (%) for partial matches
9. State codes should be uppercase (CA, TX, NY, etc.)
10. Start directly with SELECT - no preamble

QUERY OPTIMIZATION GUIDELINES:
- For "top N" queries, use ORDER BY DESC LIMIT N
- For "cheapest/most affordable", order by price ASC
- For "most expensive", order by price DESC
- For ratio analysis, exclude NULL values: WHERE column IS NOT NULL
- For state comparisons, use IN ('STATE1', 'STATE2') for multiple states
- For city searches, use LIKE '%cityname%' for partial matches

COMMON PATTERNS:
- Top expensive cities: SELECT RegionName, State, ZMediumValue FROM partners8_data WHERE ZMediumValue IS NOT NULL ORDER BY ZMediumValue DESC LIMIT 10
- Cities in specific state: WHERE State = 'CA'
- Rent-to-value analysis: WHERE ZillowRatio IS NOT NULL ORDER BY ZillowRatio DESC
- Income limit analysis: WHERE IncomeLimits IS NOT NULL

Generate the complete, executable SQL query:
"""

        try:
            response_text = self.call_gemini_api(prompt)
            sql_query = self.clean_sql_query(response_text)
            return sql_query

        except Exception as e:
            st.error(f"❌ Error generating SQL query: {e}")
            return None
    
    def get_corrected_sql_query(self, user_question, sql_query, error_message):
        """Ask Gemini to correct the SQL query with enhanced error analysis"""
        schema_prompt = self.create_schema_prompt()

        prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"

FAILED SQL QUERY:
{sql_query}

ERROR MESSAGE:
{error_message}

You are an expert SQL debugger. Analyze the error and generate a corrected SQLite query.

ERROR ANALYSIS GUIDELINES:
1. If "no such column" error: Check the exact column names in the schema above and use correct spelling/capitalization
2. If column has spaces (like "ZH Ratio", "NH Ratio"): Use double quotes around the column name
3. If syntax error: Fix SQL syntax issues (missing commas, incorrect operators, etc.)
4. If data type error: Ensure proper data type handling (numbers vs strings)
5. If the original query was asking for non-existent data: Modify to use available columns

CORRECTION REQUIREMENTS:
- Generate ONLY the corrected SQL query, no explanations
- Use exact column names from the schema (case-sensitive)
- For columns with spaces, use double quotes: "ZH Ratio", "NH Ratio"
- Ensure proper SQLite syntax
- Include appropriate NULL handling where needed
- Add reasonable LIMIT clause if missing (default 100)
- Use proper ORDER BY for meaningful results

COMMON FIXES:
- Replace incorrect column names with correct ones from schema
- Add double quotes around spaced column names
- Fix WHERE clause syntax
- Ensure proper JOIN syntax if needed
- Handle NULL values appropriately

Generate the corrected SQL query:
"""

        try:
            response_text = self.call_gemini_api(prompt)
            corrected_query = self.clean_sql_query(response_text)
            return corrected_query
        except Exception as e:
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

    def extract_missing_column(self, error_message):
        """Extract the missing column name from SQLite error message"""
        try:
            # SQLite error format: "no such column: column_name"
            if "no such column:" in error_message:
                return error_message.split("no such column:")[1].strip()
            return None
        except:
            return None

    def explain_empty_results(self, user_question, sql_query):
        """Generate explanation for why a query returned no results"""
        schema_prompt = self.create_schema_prompt()

        prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"
SQL QUERY: {sql_query}

The query executed successfully but returned no results. Analyze why this might have happened and provide a helpful explanation in natural language.

Consider these common reasons:
1. The search criteria might be too specific or restrictive
2. The requested data might not exist in the database
3. Column values might be NULL for the requested filters
4. State codes or city names might be misspelled or not in the database
5. Numeric ranges might be outside the available data range

Provide a concise, helpful explanation (1-2 sentences) that suggests what the user could try instead. Focus on what data IS available rather than what isn't.
"""

        try:
            response_text = self.call_gemini_api(prompt)
            return response_text.strip()
        except Exception:
            return "The search criteria might be too specific. Try broadening your search or checking for typos in city/state names."

    def validate_and_suggest(self, user_question):
        """Validate user question and provide suggestions if it seems problematic"""
        schema_prompt = self.create_schema_prompt()

        # Check for common issues that might lead to no results
        question_lower = user_question.lower()

        # List of data types we don't have
        unavailable_data = [
            'population', 'demographics', 'crime', 'schools', 'weather', 'employment',
            'transportation', 'hospitals', 'restaurants', 'shopping', 'parks',
            'property tax', 'mortgage rates', 'construction', 'permits'
        ]

        # Check if user is asking for unavailable data
        for unavailable in unavailable_data:
            if unavailable in question_lower:
                return {
                    'valid': False,
                    'suggestion': f"I don't have {unavailable} data. However, I can help you with rent prices, home values, income limits, and rent-to-value ratios. Try asking about these instead!"
                }

        # Use AI to validate the question
        prompt = f"""
{schema_prompt}

USER QUESTION: "{user_question}"

Analyze if this question can be answered using the available database schema above.

Return ONLY one of these responses:
1. "VALID" - if the question can be answered with available data
2. "INVALID: [brief explanation]" - if the question asks for data not in the schema

Focus on whether the requested information exists in the database columns.
"""

        try:
            response_text = self.call_gemini_api(prompt)
            response_text = response_text.strip()

            if response_text.startswith("VALID"):
                return {'valid': True, 'suggestion': None}
            elif response_text.startswith("INVALID"):
                suggestion = response_text.replace("INVALID:", "").strip()
                return {
                    'valid': False,
                    'suggestion': f"{suggestion} Try asking about rent prices, home values, income limits, or city/state comparisons instead."
                }
            else:
                return {'valid': True, 'suggestion': None}

        except Exception:
            return {'valid': True, 'suggestion': None}

    def execute_sql_query(self, user_question, sql_query):
        """Execute the SQL query and return results with enhanced error handling"""
        try:
            with sqlite3.connect(DATABASE_FILE) as conn:
                df = pd.read_sql_query(sql_query, conn)

                # Check if results are empty and provide helpful feedback
                if len(df) == 0:
                    explanation = self.explain_empty_results(user_question, sql_query)
                    st.warning("🔍 No results found for your query.")
                    st.info(f"💡 **Possible reasons:** {explanation}")

                return df

        except Exception as e:
            error_message = str(e).lower()

            # Provide user-friendly error explanations
            if "no such column" in error_message:
                st.warning("🔍 The query references a column that doesn't exist in our database.")
                missing_column = self.extract_missing_column(str(e))
                if missing_column:
                    st.info(f"💡 **Issue:** The column '{missing_column}' is not available. Please check the available columns in the sidebar or try rephrasing your question.")
            elif "syntax error" in error_message:
                st.warning("🔍 There's a syntax issue with the generated query.")
                st.info("💡 **Issue:** The AI generated an invalid SQL query. Let me try to fix it...")
            else:
                st.warning(f"🔍 Query execution failed: {e}")
                st.info("💡 **Issue:** There was a problem executing your query. Let me try to correct it...")

            # Attempt to get a corrected query
            corrected_query = self.get_corrected_sql_query(user_question, sql_query, str(e))

            if corrected_query and corrected_query != sql_query:
                st.info("🔧 **Attempting to fix the query...**")
                st.code(corrected_query, language="sql")
                try:
                    with sqlite3.connect(DATABASE_FILE) as conn:
                        df = pd.read_sql_query(corrected_query, conn)

                        if len(df) == 0:
                            explanation = self.explain_empty_results(user_question, corrected_query)
                            st.warning("🔍 The corrected query executed successfully but returned no results.")
                            st.info(f"💡 **Possible reasons:** {explanation}")
                        else:
                            st.success("✅ **Query corrected successfully!**")

                        return df

                except Exception as e2:
                    st.error("❌ **Unable to fix the query automatically.**")
                    st.info("💡 **Suggestion:** Try rephrasing your question or asking for something more specific. For example:")
                    st.info("• Instead of asking about data we don't have, ask about cities, states, rent prices, or home values")
                    st.info("• Be more specific about locations (e.g., 'California cities' instead of just 'cities')")
                    st.info("• Ask for comparisons using available data columns")
                    return None
            else:
                st.error("❌ **Unable to generate a corrected query.**")
                st.info("💡 **Suggestion:** Please try rephrasing your question using simpler terms or ask about the available data shown in the sidebar.")
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
            "What are the top 10 most expensive cities by home value?",
            "Show me California cities with rent above $2500",
            "Which states have the lowest HUD income limits?",
            "Find cities where Zillow median rent is above $3000",
            "Compare median home values between Texas and Florida",
            "Show me cities with the best rent-to-value ratios",
            "What are the most affordable cities in New York state?",
            "Find cities where 4-bedroom HUD rent is under $1500",
            "Show me the highest rent cities in each state",
            "Which cities have the biggest difference between Zillow and NAR home values?"
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
        **✅ What you CAN ask about:**
        • Zillow rent and home values
        • HUD Fair Market Rents (by bedroom count)
        • Income limits by area (HUD data)
        • Rent-to-value ratios
        • City, county, and state information
        • Price comparisons between locations
        • Rankings and top/bottom lists
        """)

        st.warning("""
        **❌ What we DON'T have:**
        • Population or demographics
        • Crime rates or safety data
        • School ratings or education data
        • Employment or job market data
        • Weather or climate information
        • Transportation or infrastructure
        • Property taxes or mortgage rates
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
            # First, validate the question
            validation = query_tool.validate_and_suggest(user_question)

            if not validation['valid']:
                st.warning("🔍 **Question Analysis**")
                st.info(f"💡 {validation['suggestion']}")
                st.info("**Try asking about:**")
                st.info("• Most expensive cities in a specific state")
                st.info("• Rent-to-value ratios by location")
                st.info("• Income limits for different areas")
                st.info("• Comparing rent prices between states")
                return

            # Generate SQL query
            sql_query = query_tool.natural_language_to_sql(user_question)

            if sql_query:
                st.subheader("📝 Generated SQL Query")
                st.code(sql_query, language="sql")

                # Execute query with enhanced error handling
                st.subheader("📊 Query Results")
                results_df = query_tool.execute_sql_query(user_question, sql_query)

                if results_df is not None and len(results_df) > 0:
                    # Show results count
                    st.success(f"✅ **Found {len(results_df):,} results**")

                    # Display results
                    st.dataframe(results_df, use_container_width=True, height=400)

                    # Download button
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name=f"partners8_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="download_csv_button"
                    )

                    # AI Summary
                    st.subheader("💡 AI Summary")
                    with st.spinner("Generating insights..."):
                        summary = query_tool.summarize_results(user_question, sql_query, results_df)
                        st.info(summary)

                    # Visualizations
                    st.subheader("📈 Visualizations")
                    charts = create_visualizations(results_df, "general")

                    if charts:
                        for chart in charts:
                            st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info("💡 No suitable visualizations could be generated for this data type.")

                elif results_df is not None and len(results_df) == 0:
                    # Empty results are handled in execute_sql_query method
                    pass
                else:
                    # Query execution failed completely
                    st.error("❌ **Unable to process your question**")
                    st.info("💡 **Try these suggestions:**")
                    st.info("• Ask about cities, states, rent prices, or home values")
                    st.info("• Use specific location names (e.g., 'California', 'Texas')")
                    st.info("• Ask for comparisons or rankings")
                    st.info("• Check the example questions in the sidebar")
            else:
                # Failed to generate SQL query
                st.error("❌ **Unable to understand your question**")
                st.info("💡 **Please try:**")
                st.info("• Using simpler language")
                st.info("• Being more specific about what you want to know")
                st.info("• Clicking one of the example questions in the sidebar")
                st.info("• Asking about the available data types shown in the sidebar")

# --- Main Application Flow ---
def main():
    """Main Streamlit application entry point."""
    init_db()

    # Clean up old session files periodically
    cleanup_old_sessions()

    # Initialize session state with more robust defaults
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['user_role'] = 'guest'
        st.session_state['user_id'] = None
        st.session_state['session_initialized'] = True

        # Try to restore session from persistent storage
        if restore_session_if_available():
            st.success("Welcome back! Your session has been restored.")
            st.rerun()

    # Add session state debugging (can be removed in production)
    if st.session_state.get('logged_in'):
        # Ensure session state is maintained
        if not st.session_state.get('username'):
            st.session_state['logged_in'] = False
    
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

        # Add session info for debugging (can be hidden in production)
        

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

