import requests
import pandas as pd
import json
import os
import time
import sqlite3
import random
import numpy as np
import logging
from datetime import datetime
from fuzzywuzzy import fuzz
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import traceback
from dotenv import load_dotenv
load_dotenv()
# =============================================================================
# CONFIGURATION
# =============================================================================

# --- Logging Configuration ---
# We will configure two handlers: one for the console and one for a file.
LOG_FILE = "scraping.log"
# Clear the log file at the start of a run
if os.path.exists(LOG_FILE):
    open(LOG_FILE, 'w').close()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Console Handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# File Handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# --- Status File ---
STATUS_FILE = "scraping_status.json"
# --- Stop Signal File ---
STOP_FILE = "stop_scraping.json" # New: File to signal stopping the pipeline

# HUD API Configuration
hud_api = os.getenv("HUD_API_KEY")
HUD_BASE_URL = os.getenv("HUD_BASE_URL")

# Validate HUD API configuration
if not hud_api:
    logger.error("❌ HUD_API_KEY not found in environment variables")
    raise ValueError("HUD_API_KEY is required but not set in environment variables")

if not HUD_BASE_URL:
    logger.error("❌ HUD_BASE_URL not found in environment variables")
    raise ValueError("HUD_BASE_URL is required but not set in environment variables")

HUD_HEADERS = {"Authorization": f"Bearer {hud_api}"}

logger.info(f"✅ HUD API configured with base URL: {HUD_BASE_URL}")

# Output directories and files
OUTPUT_DIR = "partners8_data"
ZILLOW_DIR = os.path.join(OUTPUT_DIR, "zillow_data")
FINAL_OUTPUT = "partners8_final_data.csv"

# Thread safety for HUD API calls
lock = threading.Lock()
fips_cache = {}
cache_lock = threading.Lock()
request_lock = threading.Lock()
last_request_time = 0
MIN_REQUEST_INTERVAL = 0.1

# =============================================================================
# STATUS MANAGEMENT
# =============================================================================

def update_status(status, message=None, error=None, progress=None):
    """
    Updates the status JSON file.
    Args:
        status (str): Current status (e.g., "running", "success", "failed", "stopped").
        message (str, optional): A descriptive message. Defaults to None.
        error (str, optional): Error message if status is "failed". Defaults to None.
        progress (float, optional): Progress percentage (0-100). Defaults to None.
    """
    data = {}
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                pass # Overwrite if corrupted

    data['status'] = status
    data['updated_at'] = datetime.now().isoformat()
    if message:
        data['message'] = message
    if error:
        data['error'] = error
    if progress is not None:
        data['progress'] = progress # New: Add progress field
    if status == 'running' and 'start_time' not in data: # Only set start_time once per run
        data['start_time'] = datetime.now().isoformat()
    if status in ['success', 'failed', 'stopped']: # New: Add 'stopped' to end states
        data['end_time'] = datetime.now().isoformat()
        if status == 'success':
            data['last_success_date'] = datetime.now().isoformat()
        if status == 'stopped': # New: Set progress to 100 if stopped prematurely
            data['progress'] = 100.0

    with open(STATUS_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def check_for_stop_signal():
    """
    Checks if the stop signal file exists. If it does, deletes it and returns True.
    Returns:
        bool: True if stop signal was found, False otherwise.
    """
    if os.path.exists(STOP_FILE):
        try:
            # Read stop signal details
            with open(STOP_FILE, 'r') as f:
                stop_data = json.load(f)
                requested_by = stop_data.get("requested_by", "Unknown user")
                reason = stop_data.get("reason", "No reason provided")

            logger.info(f"🛑 Stop signal detected from {requested_by}. Reason: {reason}")
            logger.info("🛑 Halting pipeline gracefully...")

            os.remove(STOP_FILE) # Remove the signal file
            update_status("stopped", message=f"Pipeline stopped by {requested_by}")

        except (json.JSONDecodeError, FileNotFoundError):
            logger.info("🛑 Stop signal detected. Halting pipeline.")
            if os.path.exists(STOP_FILE):
                os.remove(STOP_FILE)
            update_status("stopped", message="Pipeline stopped by user.")

        return True
    return False

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
        """Download Zillow ZHVI and ZORI data"""
        logger.info("🏠 Downloading Zillow data...")

        # Check for stop signal before starting
        if check_for_stop_signal():
            return False, []

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
        try:
            logger.info(f"Downloading {zillow_urls['zhvi']['description']}...")

            # Check for stop signal before download
            if check_for_stop_signal():
                return False, []

            zhvi_response = self.session.get(zillow_urls['zhvi']['url'], timeout=60)
            zhvi_response.raise_for_status()

            zhvi_filepath = os.path.join(ZILLOW_DIR, zillow_urls['zhvi']['filename'])
            with open(zhvi_filepath, 'wb') as f:
                f.write(zhvi_response.content)

            self.zhvi_data = pd.read_csv(zhvi_filepath)
            downloaded_files.append(zhvi_filepath)
            logger.info(f"✅ ZHVI data: {len(self.zhvi_data)} records")

        except Exception as e:
            logger.error(f"❌ Failed to download ZHVI data: {e}")
            return False, []

        # Check for stop signal before second download
        if check_for_stop_signal():
            return False, downloaded_files

        # Download ZORI data
        try:
            logger.info(f"Downloading {zillow_urls['zori']['description']}...")
            zori_response = self.session.get(zillow_urls['zori']['url'], timeout=60)
            zori_response.raise_for_status()

            zori_filepath = os.path.join(ZILLOW_DIR, zillow_urls['zori']['filename'])
            with open(zori_filepath, 'wb') as f:
                f.write(zori_response.content)

            self.zori_data = pd.read_csv(zori_filepath)
            downloaded_files.append(zori_filepath)
            logger.info(f"✅ ZORI data: {len(self.zori_data)} records")

        except Exception as e:
            logger.error(f"❌ Failed to download ZORI data: {e}")
            return False, downloaded_files

        logger.info("🎉 Zillow data download completed!")
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

    # Validate URL before making request
    if not url or 'None' in url:
        logger.error(f"❌ Invalid URL provided: {url}")
        return None

    session = create_session()

    for attempt in range(max_retries):
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
            error_msg = str(e)
            if 'Invalid URL' in error_msg or 'No scheme supplied' in error_msg:
                logger.error(f"❌ URL configuration error: {error_msg}")
                logger.error(f"   Check HUD_BASE_URL environment variable: {HUD_BASE_URL}")
                return None  # Don't retry for configuration errors
            else:
                logger.warning(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
    
    return None

def get_fips_code(state_code, city_name, county_name):
    """Get FIPS code with caching and rate limiting"""
    cache_key = f"{state_code}_{county_name}"

    with cache_lock:
        if cache_key in fips_cache:
            return fips_cache[cache_key]

    try:
        # Validate HUD_BASE_URL before using it
        if not HUD_BASE_URL or HUD_BASE_URL == 'None':
            logger.error("❌ HUD_BASE_URL is not properly configured")
            return None

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
    try:
        # Validate HUD_BASE_URL before using it
        if not HUD_BASE_URL or HUD_BASE_URL == 'None':
            logger.error("❌ HUD_BASE_URL is not properly configured")
            return None

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
    try:
        # Validate HUD_BASE_URL before using it
        if not HUD_BASE_URL or HUD_BASE_URL == 'None':
            logger.error("❌ HUD_BASE_URL is not properly configured")
            return None

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
    if fips_code:
        result['entityid'] = fips_code
        
        # Get FMR data
        fmr_data = get_fmr_data(fips_code)
        if fmr_data:
            result['Efficiency'] = fmr_data['Efficiency']
            result['OneBedroom'] = fmr_data['One-Bedroom']
            result['TwoBedroom'] = fmr_data['Two-Bedroom']
            result['ThreeBedroom'] = fmr_data['Three-Bedroom']
            result['FourBedroom'] = fmr_data['Four-Bedroom']
        
        # Get Income Limits
        income_limit = get_income_limits(fips_code)
        if income_limit:
            result['Income Limits'] = income_limit
    
    return result

# =============================================================================
# NAR DATA EXTRACTOR (FIXED)
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
        logger.info("🏡 Fetching NAR data from Census ACS...")

        # Check for stop signal before starting
        if check_for_stop_signal():
            return None

        try:
            url = "https://api.census.gov/data/2023/acs/acs5?get=B25077_001E,NAME&for=county:*"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # Check for stop signal after download
            if check_for_stop_signal():
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
            return census_clean

        except Exception as e:
            logger.error(f"❌ Failed to get NAR data: {e}")
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
        if census_data is None:
            logger.error("No NAR data to match")
            return 0
        
        logger.info("🔗 Matching NAR data...")
        
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
        
        # Match records
        for idx, row in data_df.iterrows():
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
        
        # Clean up temporary columns
        data_df.drop(['county_clean', 'state_full'], axis=1, inplace=True)
        census_data.drop(['county_clean', 'state_clean'], axis=1, inplace=True)
        
        logger.info(f"✅ NAR data matched: {matches}/{total_rows} ({(matches/total_rows*100):.1f}%)")
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
        logger.info("📊 Calculating ratios...")
        
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
        return data_df

# =============================================================================
# MAIN PIPELINE CLASS
# =============================================================================

class Partners8Pipeline:
    def __init__(self):
        """Initialize the complete Partners 8 data pipeline"""
        logger.info("🚀 Initializing Partners 8 Data Pipeline")
        
        # Create output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Initialize components
        self.zillow_downloader = ZillowDataDownloader()
        self.nar_extractor = NARDataExtractor()
        self.ratio_calculator = RatioCalculator()
        
        # Data container
        self.final_data = None
    
    def step1_download_zillow_data(self):
        """Step 1: Download Zillow ZHVI and ZORI data"""
        logger.info("📥 STEP 1: Downloading Zillow Data")
        update_status("running", message="Step 1: Downloading Zillow Data", progress=10) # Update progress

        # Check for stop signal before starting
        if check_for_stop_signal():
            return False

        success, files = self.zillow_downloader.download_zillow_data()
        if not success:
            raise Exception("Failed to download Zillow data")

        return success
    
    def step2_merge_zillow_data(self):
        """Step 2: Merge Zillow ZHVI and ZORI data"""
        logger.info("🔄 STEP 2: Merging Zillow Data")
        update_status("running", message="Step 2: Merging Zillow Data", progress=25) # Update progress

        # Check for stop signal before starting
        if check_for_stop_signal():
            return False

        zhvi_df = self.zillow_downloader.zhvi_data
        zori_df = self.zillow_downloader.zori_data
        
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
        return len(self.final_data)
    
    def step3_fetch_hud_data(self, max_workers=10):
        """Step 3: Fetch HUD FMR and Income Limits data"""
        logger.info("🏢 STEP 3: Fetching HUD Data")
        update_status("running", message="Step 3: Fetching HUD Data", progress=40) # Update progress
        
        # Prepare data for processing
        row_data = [(index, row) for index, row in self.final_data.iterrows()]
        
        # Process in batches
        batch_size = max_workers * 2
        total_batches = (len(row_data) + batch_size - 1) // batch_size
        
        with tqdm(total=len(row_data), desc="Fetching HUD data") as pbar:
            for batch_num in range(total_batches):
                if check_for_stop_signal(): # New: Check for stop signal
                    return 0

                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(row_data))
                batch_data = row_data[start_idx:end_idx]

                # Process batch with threading
                with ThreadPoolExecutor(max_workers=min(max_workers, len(batch_data))) as executor:
                    future_to_row = {executor.submit(process_hud_row, row): row for row in batch_data}
                    results = []

                    for future in as_completed(future_to_row):
                        # Check for stop signal more frequently during processing
                        if check_for_stop_signal():
                            # Cancel remaining futures
                            for f in future_to_row:
                                f.cancel()
                            return 0

                        try:
                            result = future.result()
                            results.append(result)
                            pbar.update(1)
                            # Update progress more frequently during this long step
                            current_progress = 40 + (pbar.n / len(row_data)) * 30 # 40% to 70% for HUD
                            update_status("running", message="Step 3: Fetching HUD Data", progress=current_progress)
                        except Exception as exc:
                            pbar.update(1)

                    # Update dataframe with results
                    for result in results:
                        if result:
                            with lock:
                                self.final_data.at[result['index'], 'entityid'] = result['entityid']
                                self.final_data.at[result['index'], 'Income Limits'] = result['Income Limits']
                                self.final_data.at[result['index'], 'Efficiency'] = result['Efficiency']
                                self.final_data.at[result['index'], 'OneBedroom'] = result['OneBedroom']
                                self.final_data.at[result['index'], 'TwoBedroom'] = result['TwoBedroom']
                                self.final_data.at[result['index'], 'ThreeBedroom'] = result['ThreeBedroom']
                                self.final_data.at[result['index'], 'FourBedroom'] = result['FourBedroom']

                # Check for stop signal before delay
                if check_for_stop_signal():
                    return 0

                # Delay between batches
                if batch_num < total_batches - 1:
                    time.sleep(1)
        
        hud_success_count = self.final_data['entityid'].notna().sum()
        logger.info(f"✅ HUD data fetched: {hud_success_count}/{len(self.final_data)} rows")
        return hud_success_count
    
    def step4_fetch_nar_data(self):
        """Step 4: Fetch NAR median home values"""
        logger.info("🏡 STEP 4: Fetching NAR Data")
        update_status("running", message="Step 4: Fetching NAR Data", progress=75) # Update progress
        if check_for_stop_signal(): # New: Check for stop signal
            return 0
        
        # Get census data
        census_data = self.nar_extractor.get_census_county_data()
        if census_data is None:
            logger.warning("⚠️  NAR data fetch failed, continuing without NAR data")
            return 0
        
        # Match and update
        matches = self.nar_extractor.match_nar_data(self.final_data, census_data)
        return matches
    
    def step5_calculate_ratios(self):
        """Step 5: Calculate all ratios"""
        logger.info("📊 STEP 5: Calculating Ratios")
        update_status("running", message="Step 5: Calculating Ratios", progress=85) # Update progress
        if check_for_stop_signal(): # New: Check for stop signal
            return False
        
        self.final_data = self.ratio_calculator.calculate_all_ratios(self.final_data)
        return True
    
    def step6_save_final_data(self):
        """Step 6: Save final dataset"""
        logger.info("💾 STEP 6: Saving Final Data")
        update_status("running", message="Step 6: Saving Final Data", progress=95) # Update progress
        if check_for_stop_signal(): # New: Check for stop signal
            return None
        
        # Save main output file
        # Clear the file first before appending
        if os.path.exists(FINAL_OUTPUT):
            os.remove(FINAL_OUTPUT)
        self.final_data.to_csv(FINAL_OUTPUT, index=False)
        
        logger.info(f"✅ Final data saved: {FINAL_OUTPUT}")
        return FINAL_OUTPUT
    
    def step7_store_in_db(self):
        """Step 7: Store the final data in SQLite DB"""
        logger.info("🗄️ STEP 7: Storing data in SQLite Database")
        update_status("running", message="Step 7: Storing data in SQLite Database", progress=98) # Update progress
        if check_for_stop_signal(): # New: Check for stop signal
            return False
        
        conn = sqlite3.connect('partners8_data.db')
        self.final_data.to_sql('partners8_data', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        logger.info("✅ Data successfully stored in partners8_data.db")
        return True
    
    def run_complete_pipeline(self):
        """Run the complete Partners 8 data pipeline"""
        logger.info("🚀 Starting Partners 8 Complete Data Pipeline")
        
        # Initial check for stop signal
        if check_for_stop_signal():
            return False
            
        # Step 1: Download Zillow data
        if not self.step1_download_zillow_data(): return False
        if check_for_stop_signal(): return False
        
        # Step 2: Merge Zillow data
        if not self.step2_merge_zillow_data(): return False
        if check_for_stop_signal(): return False
        
        # Step 3: Fetch HUD data
        if not self.step3_fetch_hud_data(): return False
        if check_for_stop_signal(): return False
        
        # Step 4: Fetch NAR data
        if not self.step4_fetch_nar_data(): return False
        if check_for_stop_signal(): return False
        
        # Step 5: Calculate ratios
        if not self.step5_calculate_ratios(): return False
        if check_for_stop_signal(): return False
        
        # Step 6: Save final data
        if not self.step6_save_final_data(): return False
        if check_for_stop_signal(): return False

        # Step 7: Store in DB
        if not self.step7_store_in_db(): return False

        logger.info("🎉 Pipeline completed successfully!")
        return True
            
# =============================================================================
# MAIN FUNCTION
# =============================================================================

def validate_configuration():
    """Validate all required configuration before starting pipeline"""
    logger.info("🔧 Validating configuration...")

    # Check environment variables
    required_vars = {
        'HUD_API_KEY': hud_api,
        'HUD_BASE_URL': HUD_BASE_URL,
        'GEMINI_API_KEY': os.getenv('GEMINI_API_KEY')
    }

    missing_vars = []
    for var_name, var_value in required_vars.items():
        if not var_value or var_value == 'None':
            missing_vars.append(var_name)

    if missing_vars:
        error_msg = f"❌ Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("✅ Configuration validation passed")
    logger.info(f"   - HUD API: {HUD_BASE_URL}")
    logger.info(f"   - Gemini API: Configured")

    return True

def cleanup_pid_file():
    """Clean up the PID file when process completes"""
    try:
        if os.path.exists("scraping_pid.json"):
            os.remove("scraping_pid.json")
    except Exception:
        pass

def main():
    """Main function to run the pipeline and handle status reporting."""
    update_status("running", message="Pipeline starting...", progress=0)

    try:
        # Validate configuration first
        validate_configuration()

        # Initialize and run pipeline
        pipeline = Partners8Pipeline()
        pipeline_success = pipeline.run_complete_pipeline()

        if pipeline_success:
            update_status("success", message="Pipeline completed successfully!", progress=100)
        else:
            # If pipeline_success is False, it means it was stopped by user or an internal check
            # The status would have been updated to "stopped" by check_for_stop_signal()
            # So, we only update to "failed" if it wasn't a graceful stop
            current_status = get_scraping_status_simple().get('status')
            if current_status != 'stopped':
                update_status("failed", message="Pipeline aborted due to an unexpected reason.", error="Unknown error during pipeline execution.")

    except Exception as e:
        error_msg = f"Pipeline failed: {e}"
        tb = traceback.format_exc()
        logger.error(error_msg)
        logger.error(tb)
        update_status("failed", message="An error occurred during the pipeline execution.", error=str(e), progress=0)

    finally:
        # Always clean up the PID file when the process ends
        cleanup_pid_file()

def get_scraping_status_simple():
    """Reads the scraping status from the JSON file for internal use."""
    if not os.path.exists(STATUS_FILE):
        return {"status": "idle", "last_success_date": None, "progress": 0}
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"status": "unknown", "last_success_date": None, "error": "Status file is corrupted or missing.", "progress": 0}

if __name__ == "__main__":
    main()

