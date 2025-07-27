import requests
import pandas as pd
import json
import os
import time
import sqlite3
import random
import numpy as np
import logging
import signal
import threading
import pickle
from datetime import datetime
from fuzzywuzzy import fuzz
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# PROGRESS TRACKING
# =============================================================================

# Progress file for communication with main process
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

# =============================================================================
# CONFIGURATION
# =============================================================================

# HUD API Configuration
HUD_API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiMzdjNDgzNmQxMWUwNTgyOTM1MWNjYTNiNjBjMTNiNDNkODA1YzZmYTYzYzcyOTAwNGZlOGRlMDUzODM1YjZkMzJmOGQ3NDNiNmUzODBkMmEiLCJpYXQiOjE3NTI0NzQ5MzIuNzg1OTk0LCJuYmYiOjE3NTI0NzQ5MzIuNzg1OTk2LCJleHAiOjIwNjgwMDc3MzIuNzgxMzYsInN1YiI6IjEwMzEwNyIsInNjb3BlcyI6W119.RG_JntBXKZhfopXSUB9ZnLzuSjUYjiTk7l0_BZSk0VuVz9kZU3ZryDEEFk-kShFflkXx0S3euYtl8gWWYR3kgg"
HUD_BASE_URL = "https://www.huduser.gov/hudapi/public"
HUD_HEADERS = {"Authorization": f"Bearer {HUD_API_KEY}"}

# Output directories and files
OUTPUT_DIR = "partners8_data"
ZILLOW_DIR = os.path.join(OUTPUT_DIR, "zillow_data")
FINAL_OUTPUT = "partners8_final_data.csv"
STATE_FILE = "pipeline_state.pkl"

# Thread safety for HUD API calls
lock = threading.Lock()
fips_cache = {}
cache_lock = threading.Lock()
request_lock = threading.Lock()
last_request_time = 0
MIN_REQUEST_INTERVAL = 0.1

# Control flags
class PipelineController:
    def __init__(self):
        self.should_stop = False
        self.current_step = 0
        self.total_steps = 6
        self.step_names = [
            "Download Zillow Data",
            "Merge Zillow Data", 
            "Fetch HUD Data",
            "Fetch NAR Data",
            "Calculate Ratios",
            "Save Final Data"
        ]
        self.lock = threading.Lock()
        
    def stop(self):
        with self.lock:
            self.should_stop = True
            write_progress_file("stopped", self.current_step, "Pipeline stopped")
            print("\n🛑 Stop signal received. Pipeline will halt gracefully...")
            
    def check_should_continue(self):
        with self.lock:
            return not self.should_stop
            
    def set_current_step(self, step, step_name):
        with self.lock:
            self.current_step = step
            write_progress_file("running", step, step_name)
            print(f"\n📍 Step {step}/{self.total_steps}: {step_name}")

# Global controller instance
controller = PipelineController()

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    controller.stop()

# =============================================================================
# STATE MANAGEMENT
# =============================================================================

class StateManager:
    def __init__(self, state_file=STATE_FILE):
        self.state_file = state_file
        
    def save_state(self, pipeline_data):
        """Save current pipeline state"""
        try:
            state = {
                'timestamp': datetime.now().isoformat(),
                'current_step': controller.current_step,
                'final_data': pipeline_data.get('final_data'),
                'zhvi_data': pipeline_data.get('zhvi_data'),
                'zori_data': pipeline_data.get('zori_data'),
                'hud_progress': pipeline_data.get('hud_progress', {}),
                'nar_progress': pipeline_data.get('nar_progress', {}),
            }
            
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
            logger.info(f"💾 State saved to {self.state_file}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save state: {e}")
    
    def load_state(self):
        """Load previous pipeline state"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'rb') as f:
                    state = pickle.load(f)
                logger.info(f"📂 State loaded from {self.state_file}")
                return state
            return None
        except Exception as e:
            logger.error(f"❌ Failed to load state: {e}")
            return None
    
    def clear_state(self):
        """Clear saved state"""
        try:
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
                logger.info("🗑️ Previous state cleared")
        except Exception as e:
            logger.error(f"❌ Failed to clear state: {e}")

# =============================================================================
# ZILLOW DATA DOWNLOADER
# =============================================================================

class ZillowDataDownloader:
    def __init__(self):
        """Initialize Zillow data downloader"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create directories
        os.makedirs(ZILLOW_DIR, exist_ok=True)
        
        self.zhvi_data = None
        self.zori_data = None
    
    def download_zillow_data(self):
        """Download Zillow ZHVI and ZORI data with stop checks"""
        if not controller.check_should_continue():
            return False, []
            
        logger.info("🏠 Downloading Zillow data...")
        write_progress_file("running", 1, "Starting Zillow download...")
        
        zillow_urls = {
            'zhvi': {
                'url': 'https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
                'filename': 'zillow_zhvi_City_home_values.csv',
                'description': 'Zillow Home Value Index - City Level'
            },
            'zori': {
                'url': 'https://files.zillowstatic.com/research/public_csvs/zori/City_zori_uc_sfrcondomfr_sm_month.csv',
                'filename': 'zillow_zori_City_rent_values.csv',
                'description': 'Zillow Observed Rent Index - City Level'
            }
        }
        
        downloaded_files = []
        
        # Download ZHVI data
        if not controller.check_should_continue():
            return False, []
            
        try:
            logger.info(f"Downloading {zillow_urls['zhvi']['description']}...")
            write_progress_file("running", 1, "Downloading ZHVI data...")
            
            zhvi_response = self.session.get(zillow_urls['zhvi']['url'], timeout=60)
            zhvi_response.raise_for_status()
            
            if not controller.check_should_continue():
                return False, []
            
            zhvi_filepath = os.path.join(ZILLOW_DIR, zillow_urls['zhvi']['filename'])
            with open(zhvi_filepath, 'wb') as f:
                f.write(zhvi_response.content)
            
            self.zhvi_data = pd.read_csv(zhvi_filepath)
            downloaded_files.append(zhvi_filepath)
            logger.info(f"✅ ZHVI data: {len(self.zhvi_data)} records")
            write_progress_file("running", 1, f"ZHVI downloaded: {len(self.zhvi_data)} records")
            
        except Exception as e:
            logger.error(f"❌ Failed to download ZHVI data: {e}")
            write_progress_file("failed", 1, f"ZHVI download failed: {str(e)}")
            return False, []
        
        # Download ZORI data
        if not controller.check_should_continue():
            return False, downloaded_files
            
        try:
            logger.info(f"Downloading {zillow_urls['zori']['description']}...")
            write_progress_file("running", 1, "Downloading ZORI data...")
            
            zori_response = self.session.get(zillow_urls['zori']['url'], timeout=60)
            zori_response.raise_for_status()
            
            if not controller.check_should_continue():
                return False, downloaded_files
            
            zori_filepath = os.path.join(ZILLOW_DIR, zillow_urls['zori']['filename'])
            with open(zori_filepath, 'wb') as f:
                f.write(zori_response.content)
            
            self.zori_data = pd.read_csv(zori_filepath)
            downloaded_files.append(zori_filepath)
            logger.info(f"✅ ZORI data: {len(self.zori_data)} records")
            write_progress_file("running", 1, f"ZORI downloaded: {len(self.zori_data)} records")
            
        except Exception as e:
            logger.error(f"❌ Failed to download ZORI data: {e}")
            write_progress_file("failed", 1, f"ZORI download failed: {str(e)}")
            return False, downloaded_files
        
        logger.info("🎉 Zillow data download completed!")
        write_progress_file("running", 1, "Zillow data download completed")
        return True, downloaded_files

# =============================================================================
# HUD DATA FETCHER
# =============================================================================

def create_session():
    """Create a session with retry strategy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def rate_limited_request(url, headers, timeout=30, max_retries=3):
    """Make a rate-limited request with exponential backoff"""
    global last_request_time
    
    if not controller.check_should_continue():
        return None
    
    session = create_session()
    
    for attempt in range(max_retries):
        if not controller.check_should_continue():
            return None
            
        with request_lock:
            current_time = time.time()
            time_since_last = current_time - last_request_time
            if time_since_last < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
            last_request_time = time.time()
        
        try:
            response = session.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Rate limited. Waiting {wait_time:.2f}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
                continue
            else:
                logger.warning(f"Request failed with status {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            continue
    
    return None

def get_fips_code(state_code, city_name, county_name):
    """Get FIPS code with caching and rate limiting"""
    if not controller.check_should_continue():
        return None
        
    cache_key = f"{state_code}_{county_name}"
    
    with cache_lock:
        if cache_key in fips_cache:
            return fips_cache[cache_key]
    
    try:
        url = f"{HUD_BASE_URL}/fmr/listCounties/{state_code}?updated=2025"
        response = rate_limited_request(url, HUD_HEADERS)
        
        if response is None:
            return None
        
        json_response = response.json()
        
        if isinstance(json_response, dict):
            counties = json_response.get('data', [])
        elif isinstance(json_response, list):
            counties = json_response
        else:
            return None
        
        normalized_county_name = ' '.join(county_name.strip().split()).lower()
        
        fips_result = None
        for area in counties:
            api_county_name = area.get('cntyname', '').strip().lower()
            if api_county_name == normalized_county_name:
                fips_result = area.get('fips_code')
                break
            similarity = fuzz.ratio(api_county_name, normalized_county_name)
            if similarity > 75:
                fips_result = area.get('fips_code')
                break
        
        with cache_lock:
            fips_cache[cache_key] = fips_result
        
        return fips_result
        
    except Exception as e:
        logger.debug(f"Error fetching FIPS for {state_code}, {city_name}: {e}")
        return None

def get_fmr_data(entityid, year="2025"):
    """Get Fair Market Rent data with rate limiting"""
    if not controller.check_should_continue():
        return None
        
    try:
        url = f"{HUD_BASE_URL}/fmr/data/{entityid}?year={year}"
        response = rate_limited_request(url, HUD_HEADERS)
        
        if response is None:
            return None
            
        data = response.json().get('data', {})
        basicdata = data.get('basicdata', {})
        if isinstance(basicdata, list):
            basicdata = next((item for item in basicdata if item.get('zip_code') == 'MSA level'), basicdata[0])
        return {
            'Efficiency': float(basicdata.get('Efficiency', pd.NA)),
            'One-Bedroom': float(basicdata.get('One-Bedroom', pd.NA)),
            'Two-Bedroom': float(basicdata.get('Two-Bedroom', pd.NA)),
            'Three-Bedroom': float(basicdata.get('Three-Bedroom', pd.NA)),
            'Four-Bedroom': float(basicdata.get('Four-Bedroom', pd.NA))
        }
    except Exception as e:
        logger.debug(f"Error fetching FMR for {entityid}: {e}")
        return None

def get_income_limits(entityid, year="2025"):
    """Get income limits data with rate limiting"""
    if not controller.check_should_continue():
        return None
        
    try:
        url = f"{HUD_BASE_URL}/il/data/{entityid}?year={year}"
        response = rate_limited_request(url, HUD_HEADERS)
        
        if response is None:
            return None
            
        data = response.json().get('data', {})
        very_low = data.get('very_low', {})
        return float(very_low.get('il50_p4', pd.NA))
    except Exception as e:
        logger.debug(f"Error fetching Income Limits for {entityid}: {e}")
        return None

def process_hud_row(row_data):
    """Process a single row - fetch FIPS, FMR, and Income Limits"""
    if not controller.check_should_continue():
        return None
        
    index, row = row_data
    state_code = row['State']
    city_name = row['City']
    county_name = row['County']
    
    result = {
        'index': index,
        'entityid': None,
        'Income Limits': pd.NA,
        'Efficiency': pd.NA,
        'OneBedroom': pd.NA,
        'TwoBedroom': pd.NA,
        'ThreeBedroom': pd.NA,
        'FourBedroom': pd.NA
    }
    
    # Get FIPS code
    fips_code = get_fips_code(state_code, city_name, county_name)
    if fips_code and controller.check_should_continue():
        result['entityid'] = fips_code
        
        # Get FMR data
        fmr_data = get_fmr_data(fips_code)
        if fmr_data and controller.check_should_continue():
            result['Efficiency'] = fmr_data['Efficiency']
            result['OneBedroom'] = fmr_data['One-Bedroom']
            result['TwoBedroom'] = fmr_data['Two-Bedroom']
            result['ThreeBedroom'] = fmr_data['Three-Bedroom']
            result['FourBedroom'] = fmr_data['Four-Bedroom']
        
        # Get Income Limits
        income_limit = get_income_limits(fips_code)
        if income_limit and controller.check_should_continue():
            result['Income Limits'] = income_limit
    
    return result

# =============================================================================
# NAR DATA EXTRACTOR
# =============================================================================

class NARDataExtractor:
    def __init__(self):
        """Initialize NAR data extractor"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # State abbreviation to full name mapping
        self.state_mapping = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
            'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
            'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
            'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
            'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
            'DC': 'District of Columbia'
        }
    
    def get_census_county_data(self):
        """Get median home values from Census ACS data (NAR's source)"""
        if not controller.check_should_continue():
            return None
            
        logger.info("🏡 Fetching NAR data from Census ACS...")
        write_progress_file("running", 4, "Fetching Census data...")
        
        try:
            url = "https://api.census.gov/data/2023/acs/acs5?get=B25077_001E,NAME&for=county:*"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            if not controller.check_should_continue():
                return None
            
            data = response.json()
            headers = data[0]
            rows = data[1:]
            census_df = pd.DataFrame(rows, columns=headers)
            
            # Clean and parse data
            census_df['NMediumValue'] = pd.to_numeric(census_df['B25077_001E'], errors='coerce')
            county_state = census_df['NAME'].str.extract(r'(.+) County, (.+)')
            census_df['County'] = county_state[0]
            census_df['State'] = county_state[1]
            
            # Remove null values
            census_df = census_df.dropna(subset=['NMediumValue', 'County', 'State'])
            census_clean = census_df[['County', 'State', 'NMediumValue']].copy()
            
            logger.info(f"✅ Retrieved NAR data for {len(census_clean)} counties")
            write_progress_file("running", 4, f"Census data retrieved: {len(census_clean)} counties")
            return census_clean
            
        except Exception as e:
            logger.error(f"❌ Failed to get NAR data: {e}")
            write_progress_file("failed", 4, f"Census data failed: {str(e)}")
            return None
    
    def normalize_county_name(self, name):
        """Normalize county names for better matching"""
        if pd.isna(name):
            return ""
        
        # Convert to string and normalize
        normalized = str(name).strip().lower()
        
        # Remove common suffixes
        suffixes_to_remove = [' county', ' parish', ' borough', ' census area', ' city and borough']
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
                break
        
        # Handle special cases
        replacements = {
            'st.': 'saint',
            'st ': 'saint ',
            'ste.': 'sainte',
            'ste ': 'sainte ',
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        return normalized.strip()
    
    def normalize_state_name(self, state_abbrev):
        """Convert state abbreviation to full name"""
        if pd.isna(state_abbrev):
            return ""
        
        state_str = str(state_abbrev).strip().upper()
        return self.state_mapping.get(state_str, state_str)
    
    def match_nar_data(self, data_df, census_data):
        """Match census data to existing data and update NMediumValue"""
        if census_data is None or not controller.check_should_continue():
            logger.error("No NAR data to match")
            return 0
        
        logger.info("🔗 Matching NAR data...")
        write_progress_file("running", 4, "Matching NAR data to cities...")
        
        # Prepare data for matching
        data_df['county_clean'] = data_df['County'].apply(self.normalize_county_name)
        data_df['state_full'] = data_df['State'].apply(self.normalize_state_name)
        
        census_data['county_clean'] = census_data['County'].apply(self.normalize_county_name)
        census_data['state_clean'] = census_data['State'].apply(lambda x: str(x).strip())
        
        # Initialize NMediumValue column if it doesn't exist or reset it
        data_df['NMediumValue'] = pd.NA
        
        matches = 0
        total_rows = len(data_df)
        
        # Create a lookup dictionary for faster matching
        census_lookup = {}
        for _, row in census_data.iterrows():
            key = (row['county_clean'], row['state_clean'])
            census_lookup[key] = row['NMediumValue']
        
        # Match records with progress checking
        for idx, row in data_df.iterrows():
            if not controller.check_should_continue():
                break
                
            if pd.notna(row['county_clean']) and pd.notna(row['state_full']):
                county_clean = row['county_clean']
                state_full = row['state_full']
                
                # Try exact match first
                lookup_key = (county_clean, state_full)
                if lookup_key in census_lookup:
                    data_df.at[idx, 'NMediumValue'] = census_lookup[lookup_key]
                    matches += 1
                    continue
                
                # Try fuzzy matching within same state
                state_data = census_data[census_data['state_clean'] == state_full]
                if len(state_data) == 0:
                    continue
                
                best_match = None
                best_score = 0
                
                for _, census_row in state_data.iterrows():
                    similarity = fuzz.ratio(county_clean, census_row['county_clean'])
                    if similarity > best_score and similarity > 80:  # 80% similarity threshold
                        best_score = similarity
                        best_match = census_row
                
                if best_match is not None:
                    data_df.at[idx, 'NMediumValue'] = best_match['NMediumValue']
                    matches += 1
                    
                    # Log fuzzy matches for debugging
                    if best_score < 100:
                        logger.debug(f"Fuzzy match ({best_score}%): '{county_clean}' -> '{best_match['county_clean']}' in {state_full}")
            
            # Update progress periodically
            if idx % 1000 == 0:
                progress = f"Matching NAR data: {idx}/{total_rows} processed"
                write_progress_file("running", 4, progress, matches)
        
        # Clean up temporary columns
        data_df.drop(['county_clean', 'state_full'], axis=1, inplace=True)
        census_data.drop(['county_clean', 'state_clean'], axis=1, inplace=True)
        
        logger.info(f"✅ NAR data matched: {matches}/{total_rows} ({(matches/total_rows*100):.1f}%)")
        write_progress_file("running", 4, f"NAR matching completed: {matches} matches")
        return matches

# =============================================================================
# RATIO CALCULATOR
# =============================================================================

class RatioCalculator:
    def __init__(self):
        """Initialize ratio calculator"""
        pass
    
    def calculate_all_ratios(self, data_df):
        """Calculate all required ratios"""
        if not controller.check_should_continue():
            return data_df
            
        logger.info("📊 Calculating ratios...")
        write_progress_file("running", 5, "Calculating financial ratios...")
        
        # Convert to numeric
        numeric_columns = ['ZMediumRent', 'ZMediumValue', 'NMediumValue', 'FourBedroom']
        for col in numeric_columns:
            if col in data_df.columns:
                data_df[col] = pd.to_numeric(data_df[col], errors='coerce')
        
        # Calculate ratios
        data_df['Zillow Ratio'] = data_df['ZMediumRent'] / data_df['ZMediumValue']
        data_df['NAR Ratio'] = data_df['ZMediumRent'] / data_df['NMediumValue']
        data_df['ZH Ratio'] = data_df['FourBedroom'] / data_df['ZMediumValue']
        data_df['NH Ratio'] = data_df['FourBedroom'] / data_df['NMediumValue']
        
        # Clean infinite values
        ratio_columns = ['Zillow Ratio', 'NAR Ratio', 'ZH Ratio', 'NH Ratio']
        for col in ratio_columns:
            infinite_mask = np.isinf(data_df[col])
            data_df.loc[infinite_mask, col] = np.nan
        
        # Log results
        for col in ratio_columns:
            valid_count = data_df[col].notna().sum()
            logger.info(f"  {col}: {valid_count} valid calculations")
        
        logger.info("✅ All ratios calculated")
        write_progress_file("running", 5, "All ratios calculated successfully")
        return data_df

# =============================================================================
# MAIN PIPELINE CLASS
# =============================================================================

class Partners8Pipeline:
    def __init__(self):
        """Initialize the complete Partners 8 data pipeline"""
        logger.info("🚀 Initializing Partners 8 Data Pipeline")
        write_progress_file("running", 0, "Initializing pipeline")
        
        # Create output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Initialize components
        self.zillow_downloader = ZillowDataDownloader()
        self.nar_extractor = NARDataExtractor()
        self.ratio_calculator = RatioCalculator()
        self.state_manager = StateManager()
        
        # Data container
        self.final_data = None
        self.resume_from_step = 0
    
    def load_previous_state(self):
        """Load previous pipeline state if available"""
        state = self.state_manager.load_state()
        if state:
            self.resume_from_step = state.get('current_step', 0)
            self.final_data = state.get('final_data')
            if state.get('zhvi_data') is not None:
                self.zillow_downloader.zhvi_data = state.get('zhvi_data')
            if state.get('zori_data') is not None:
                self.zillow_downloader.zori_data = state.get('zori_data')
            
            print(f"📂 Resuming from step {self.resume_from_step}: {controller.step_names[self.resume_from_step-1] if self.resume_from_step > 0 else 'Beginning'}")
            return True
        return False
    
    def save_current_state(self):
        """Save current pipeline state"""
        pipeline_data = {
            'final_data': self.final_data,
            'zhvi_data': self.zillow_downloader.zhvi_data,
            'zori_data': self.zillow_downloader.zori_data,
        }
        self.state_manager.save_state(pipeline_data)
    
    def step1_download_zillow_data(self):
        """Step 1: Download Zillow ZHVI and ZORI data"""
        controller.set_current_step(1, "Download Zillow Data")
        
        if self.resume_from_step >= 1:
            logger.info("📥 STEP 1: Zillow Data (Previously Downloaded)")
            write_progress_file("running", 1, "Zillow Data (Previously Downloaded)")
            return True
        
        logger.info("📥 STEP 1: Downloading Zillow Data")
        write_progress_file("running", 1, "Starting Zillow data download...")
        
        success, files = self.zillow_downloader.download_zillow_data()
        if not success or not controller.check_should_continue():
            write_progress_file("failed", 1, "Failed to download Zillow data")
            return False
        
        write_progress_file("running", 1, "Zillow data downloaded successfully")
        self.save_current_state()
        return success
    
    def step2_merge_zillow_data(self):
        """Step 2: Merge Zillow ZHVI and ZORI data"""
        controller.set_current_step(2, "Merge Zillow Data")
        
        if self.resume_from_step >= 2:
            logger.info("🔄 STEP 2: Zillow Data (Previously Merged)")
            write_progress_file("running", 2, "Zillow Data (Previously Merged)")
            return len(self.final_data) if self.final_data is not None else 0
        
        if not controller.check_should_continue():
            return 0
            
        logger.info("🔄 STEP 2: Merging Zillow Data")
        write_progress_file("running", 2, "Merging Zillow ZHVI and ZORI data...")
        
        zhvi_df = self.zillow_downloader.zhvi_data
        zori_df = self.zillow_downloader.zori_data
        
        if zhvi_df is None or zori_df is None:
            logger.error("❌ Zillow data not available for merging")
            write_progress_file("failed", 2, "Zillow data not available for merging")
            return 0
        
        # Define columns for final dataset
        columns = [
            'Region', 'SizeRank', 'RegionName', 'State', 'County', 'City',
            'ZMediumRent', 'ZMediumValue', 'NMediumValue', 'entityid',
            'Income Limits', 'Efficiency', 'OneBedroom', 'TwoBedroom',
            'ThreeBedroom', 'FourBedroom', 'Zillow Ratio', 'NAR Ratio',
            'ZH Ratio', 'NH Ratio'
        ]
        
        # Merge on common columns
        merged_df = pd.merge(
            zhvi_df[['RegionID', 'SizeRank', 'RegionName', 'State', 'CountyName']],
            zori_df[['RegionID', 'RegionName', 'State', 'CountyName', zori_df.columns[-1]]],
            on=['RegionID', 'RegionName', 'State', 'CountyName'],
            how='inner'
        )
        
        if not controller.check_should_continue():
            return 0
        
        # Create final dataframe
        self.final_data = pd.DataFrame(columns=columns)
        self.final_data['Region'] = merged_df['RegionID']
        self.final_data['SizeRank'] = merged_df['SizeRank']
        self.final_data['RegionName'] = merged_df['RegionName']
        self.final_data['State'] = merged_df['State']
        self.final_data['County'] = merged_df['CountyName']
        self.final_data['City'] = merged_df['RegionName']
        self.final_data['ZMediumRent'] = merged_df[zori_df.columns[-1]]
        self.final_data['ZMediumValue'] = zhvi_df[zhvi_df['RegionID'].isin(merged_df['RegionID'])][zhvi_df.columns[-1]].values
        
        # Initialize other columns
        other_cols = ['NMediumValue', 'entityid', 'Income Limits', 'Efficiency', 'OneBedroom', 
                     'TwoBedroom', 'ThreeBedroom', 'FourBedroom', 'Zillow Ratio', 
                     'NAR Ratio', 'ZH Ratio', 'NH Ratio']
        self.final_data[other_cols] = pd.NA
        
        logger.info(f"✅ Merged data: {len(self.final_data)} rows")
        write_progress_file("running", 2, f"Data merged successfully: {len(self.final_data)} records", len(self.final_data))
        self.save_current_state()
        return len(self.final_data)
    
    def step3_fetch_hud_data(self, max_workers=10):
        """Step 3: Fetch HUD FMR and Income Limits data"""
        controller.set_current_step(3, "Fetch HUD Data")
        
        if self.resume_from_step >= 3:
            logger.info("🏢 STEP 3: HUD Data (Previously Fetched)")
            write_progress_file("running", 3, "HUD Data (Previously Fetched)")
            return self.final_data['entityid'].notna().sum() if self.final_data is not None else 0
        
        if not controller.check_should_continue():
            return 0
            
        logger.info("🏢 STEP 3: Fetching HUD Data")
        write_progress_file("running", 3, "Starting HUD data fetch...")
        
        if self.final_data is None or len(self.final_data) == 0:
            logger.error("❌ No data available for HUD processing")
            write_progress_file("failed", 3, "No data available for HUD processing")
            return 0
        
        # Prepare data for processing
        row_data = [(index, row) for index, row in self.final_data.iterrows()]
        
        # Process in batches
        batch_size = max_workers * 2
        total_batches = (len(row_data) + batch_size - 1) // batch_size
        
        with tqdm(total=len(row_data), desc="Fetching HUD data") as pbar:
            for batch_num in range(total_batches):
                if not controller.check_should_continue():
                    break
                    
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(row_data))
                batch_data = row_data[start_idx:end_idx]
                
                # Update progress
                progress_percent = (batch_num / total_batches) * 100
                records_so_far = start_idx
                write_progress_file("running", 3, f"Fetching HUD data: {progress_percent:.1f}% complete", records_so_far)
                
                # Process batch with threading
                with ThreadPoolExecutor(max_workers=min(max_workers, len(batch_data))) as executor:
                    future_to_row = {executor.submit(process_hud_row, row): row for row in batch_data}
                    results = []
                    
                    for future in as_completed(future_to_row):
                        if not controller.check_should_continue():
                            # Cancel remaining futures
                            for f in future_to_row:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result()
                            results.append(result)
                            pbar.update(1)
                        except Exception as exc:
                            pbar.update(1)
                    
                    # Update dataframe with results
                    for result in results:
                        if result and controller.check_should_continue():
                            with lock:
                                self.final_data.at[result['index'], 'entityid'] = result['entityid']
                                self.final_data.at[result['index'], 'Income Limits'] = result['Income Limits']
                                self.final_data.at[result['index'], 'Efficiency'] = result['Efficiency']
                                self.final_data.at[result['index'], 'OneBedroom'] = result['OneBedroom']
                                self.final_data.at[result['index'], 'TwoBedroom'] = result['TwoBedroom']
                                self.final_data.at[result['index'], 'ThreeBedroom'] = result['ThreeBedroom']
                                self.final_data.at[result['index'], 'FourBedroom'] = result['FourBedroom']
                
                # Save progress and delay between batches
                if batch_num % 5 == 0:  # Save every 5 batches
                    self.save_current_state()
                
                if batch_num < total_batches - 1 and controller.check_should_continue():
                    time.sleep(1)
        
        hud_success_count = self.final_data['entityid'].notna().sum()
        logger.info(f"✅ HUD data fetched: {hud_success_count}/{len(self.final_data)} rows")
        write_progress_file("running", 3, f"HUD data completed: {hud_success_count} records", hud_success_count)
        self.save_current_state()
        return hud_success_count
    
    def step4_fetch_nar_data(self):
        """Step 4: Fetch NAR median home values"""
        controller.set_current_step(4, "Fetch NAR Data")
        
        if self.resume_from_step >= 4:
            logger.info("🏡 STEP 4: NAR Data (Previously Fetched)")
            write_progress_file("running", 4, "NAR Data (Previously Fetched)")
            return self.final_data['NMediumValue'].notna().sum() if self.final_data is not None else 0
        
        if not controller.check_should_continue():
            return 0
            
        logger.info("🏡 STEP 4: Fetching NAR Data")
        write_progress_file("running", 4, "Starting NAR data fetch...")
        
        if self.final_data is None or len(self.final_data) == 0:
            logger.error("❌ No data available for NAR processing")
            write_progress_file("failed", 4, "No data available for NAR processing")
            return 0
        
        # Get census data
        census_data = self.nar_extractor.get_census_county_data()
        if census_data is None or not controller.check_should_continue():
            logger.warning("⚠️  NAR data fetch failed, continuing without NAR data")
            write_progress_file("running", 4, "NAR data fetch failed, skipping...")
            return 0
        
        # Match and update
        matches = self.nar_extractor.match_nar_data(self.final_data, census_data)
        write_progress_file("running", 4, f"NAR data completed: {matches} matches", matches)
        self.save_current_state()
        return matches
    
    def step5_calculate_ratios(self):
        """Step 5: Calculate all ratios"""
        controller.set_current_step(5, "Calculate Ratios")
        
        if self.resume_from_step >= 5:
            logger.info("📊 STEP 5: Ratios (Previously Calculated)")
            write_progress_file("running", 5, "Ratios (Previously Calculated)")
            return True
        
        if not controller.check_should_continue():
            return False
            
        logger.info("📊 STEP 5: Calculating Ratios")
        write_progress_file("running", 5, "Starting ratio calculations...")
        
        if self.final_data is None or len(self.final_data) == 0:
            logger.error("❌ No data available for ratio calculation")
            write_progress_file("failed", 5, "No data available for ratio calculation")
            return False
        
        self.final_data = self.ratio_calculator.calculate_all_ratios(self.final_data)
        write_progress_file("running", 5, "All ratios calculated successfully")
        self.save_current_state()
        return True
    
    def step6_save_final_data(self):
        """Step 6: Save final dataset"""
        controller.set_current_step(6, "Save Final Data")
        
        if not controller.check_should_continue():
            return None
            
        logger.info("💾 STEP 6: Saving Final Data")
        write_progress_file("running", 6, "Saving final dataset...")
        
        if self.final_data is None or len(self.final_data) == 0:
            logger.error("❌ No data available to save")
            write_progress_file("failed", 6, "No data available to save")
            return None
        
        # Save main output file
        self.final_data.to_csv(FINAL_OUTPUT, index=False)
        
        logger.info(f"✅ Final data saved: {FINAL_OUTPUT}")
        write_progress_file("running", 6, f"CSV saved: {FINAL_OUTPUT}")
        
        # Display quick stats
        print("\n" + "="*60)
        print("PARTNERS 8 DATA PIPELINE COMPLETED!")
        print("="*60)
        print(f"📁 Final dataset: {FINAL_OUTPUT}")
        print(f"📊 Total records: {len(self.final_data):,}")
        print(f"🏠 Zillow coverage: {self.final_data['ZMediumRent'].notna().sum():,} ({(self.final_data['ZMediumRent'].notna().sum()/len(self.final_data)*100):.1f}%)")
        print(f"🏢 HUD coverage: {self.final_data['entityid'].notna().sum():,} ({(self.final_data['entityid'].notna().sum()/len(self.final_data)*100):.1f}%)")
        print(f"🏡 NAR coverage: {self.final_data['NMediumValue'].notna().sum():,} ({(self.final_data['NMediumValue'].notna().sum()/len(self.final_data)*100):.1f}%)")
        print("="*60)
        
        return FINAL_OUTPUT
    
    def csv_sqlite(self):
        """Store the CSV data in SQLite database table"""
        if not controller.check_should_continue():
            return
            
        logger.info("💾 Storing data in SQLite database...")
        write_progress_file("running", 6, "Storing data in SQLite database...")
        
        try:
            df = pd.read_csv(FINAL_OUTPUT) if os.path.exists(FINAL_OUTPUT) else self.final_data
            if df is None or len(df) == 0:
                logger.error("❌ No data available for database storage")
                write_progress_file("failed", 6, "No data available for database storage")
                return
            
            conn = sqlite3.connect('partners8_data.db')
            df_copy = df.copy()
            df_copy.rename(columns={'Region': 'ZipCode'}, inplace=True)
            df_copy.to_sql('partners8_data', conn, if_exists='replace', index=False)
            conn.commit()
            conn.close()
            
            logger.info("✅ Data successfully stored in SQLite database")
            write_progress_file("running", 6, "Data stored in SQLite database")
            
        except Exception as e:
            logger.error(f"❌ Failed to store data in database: {e}")
            write_progress_file("failed", 6, f"Database storage failed: {str(e)}")
    
    def run_complete_pipeline(self):
        """Run the complete Partners 8 data pipeline"""
        try:
            logger.info("🚀 Starting Partners 8 Complete Data Pipeline")
            write_progress_file("running", 0, "Starting pipeline...")
            
            if not self.step1_download_zillow_data(): return False
            # Step 2: Merge Zillow data
            if not self.step2_merge_zillow_data(): return False

            # Step 3: Fetch HUD data
            if not self.step3_fetch_hud_data(): return False

            # Step 4: Fetch NAR data
            if not self.step4_fetch_nar_data(): return False

            # Step 5: Calculate ratios
            if not self.step5_calculate_ratios(): return False

            # Step 6: Save final data
            if not self.step6_save_final_data(): return False

            # Save to SQLite
            self.csv_sqlite()
            
            # Final success
            final_records = len(self.final_data)
            write_progress_file("completed", 6, "Pipeline completed successfully", final_records)
            
            # Clear state file on successful completion
            self.state_manager.clear_state()
            
            # Clean up progress file
            try:
                if os.path.exists(PROGRESS_FILE):
                    os.remove(PROGRESS_FILE)
            except:
                pass
            
            logger.info("🎉 Pipeline completed successfully!")
            return "Pipeline completed successfully!"
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            write_progress_file("failed", controller.current_step, f"Pipeline failed: {str(e)}")
            self.save_current_state()
            raise

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main function - simplified without pause/play controls"""
    print("🏠 Partners 8 App - Data Pipeline")
    print("=" * 60)
    print("This script will:")
    print("1. Download latest Zillow ZHVI and ZORI data")
    print("2. Fetch HUD Fair Market Rent and Income Limits")
    print("3. Extract NAR median home values from Census")
    print("4. Calculate all required ratios")
    print("5. Generate final CSV for Partners 8 web app")
    print("6. Store data in SQLite database")
    print("=" * 60)
    
    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Initialize and run pipeline
        pipeline = Partners8Pipeline()
        
        # Check if user wants to resume from previous state
        state_manager = StateManager()
        
        print("\n🚀 Starting pipeline...")
        result = pipeline.run_complete_pipeline()
        
        if "stopped" in result.lower():
            print(f"\n⏹️  {result}")
            print("💾 Progress has been saved. Run the script again to resume.")
        else:
            print(f"\n🎉 {result}")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Pipeline interrupted by user")
        print("💾 Progress has been saved. Run the script again to resume.")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        print("💾 Progress has been saved. Run the script again to resume.")
        logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()