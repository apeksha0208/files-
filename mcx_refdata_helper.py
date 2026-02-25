#!/usr/bin/env python3
import os
import json
import pandas as pd
import requests
from urllib.parse import quote
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List
from calendar import month_name, month_abbr
from urllib.parse import unquote
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('/opt/titan_data/refdata_mcx/.env')

@dataclass
class MCXConfig:
    """Centralized configuration for MCX SFTP credentials and file paths"""
    
    # SFTP Credentials - loaded from environment variables
    BASE_URL: str = "https://sftp.mcxindia.com"
    USER: str = os.getenv("MCX_USER")  
    PASSWORD: str = os.getenv("MCX_PASSWORD")  
    
    # Base directory structure
    BASE_DIR: Path = Path("/opt/titan_data/refdata_mcx/")
    
    # Remote paths
    REMOTE_PATHS: Dict[str, str] = None
    
    # Local directory structure
    LOCAL_DIRS: Dict[str, Path] = None
    
    def __post_init__(self):
        # Validate that credentials are set (only if they're actually needed)
        if self.USER is None:
            self.USER = ""
        if self.PASSWORD is None:
            self.PASSWORD = ""
        
        # Initialize remote paths
        if self.REMOTE_PATHS is None:
            self.REMOTE_PATHS = {
                "bhavcopy": "/Common/Contract Master/MTSBhavcopy/",
                "contract_master": "/Common/Contract Master/CTCLContractMaster/", 
                "span_files": "/Common/RPFFile",
                "product_master": "/Common/", 

            }
        
        # Initialize local directories
        if self.LOCAL_DIRS is None:
            self.LOCAL_DIRS = {
                "raw_files": self.BASE_DIR / "raw_files",
                "span": self.BASE_DIR / "span",
                "refdata": self.BASE_DIR / "refdata"
            }
        
        # Ensure all directories exist
        self.ensure_directories()
    
    def ensure_directories(self):
        """Create all required directories"""
        for directory in self.LOCAL_DIRS.values():
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_bhavcopy_path(self, trade_date: str) -> Path:
         raw_date_dir = self.LOCAL_DIRS["raw_files"] / trade_date
         return raw_date_dir / f"bhavcopy_{trade_date}.csv"
    
    def get_ctcl_dir(self, trade_date: str) -> Path:
        """Get CTCL contract master directory for given trade date"""
        raw_date_dir = self.LOCAL_DIRS["raw_files"] / trade_date
        return raw_date_dir
    
    def get_contract_files(self) -> List[str]:
        """Get list of contract master files to download"""
        return ["MCXScrips.bcp", "MCX_ASSET_MASTER.bcp", "MCX_PART.bcp","MCX_ProductMaster.csv"]
    
    def get_product_master_path(self, trade_date: str) -> Path:
        """Get product master file path for given trade date"""
        raw_date_dir = self.LOCAL_DIRS["raw_files"] / trade_date
        return raw_date_dir / "MCX_ProductMaster.csv"
    
    def get_refdata_path(self, trade_date: str) -> Path:
        """Get refdata output path"""
        return self.LOCAL_DIRS["refdata"] / f"RefData_{trade_date}.csv"

# Global configuration instance
mcx_config = MCXConfig()

# ProductName to Stream mapping (based on MCX stream configuration table)
# This is the ONLY mapping used for stream ID assignment.
# Maps ProductName directly to stream ID (e.g., "1", "2", "3", "4", "5")
PRODUCT_NAME_TO_STREAM_MAP = {
    # Stream 1 products
    "OFGOLD": "1",
    "FUGOLD": "1",
    "FUGOLDGUINEA": "1",
    "FUGOLDM": "1",
    "FUGOLDPETAL": "1",
    "FUMCXBULLDEX": "1",
    "FUCRUDEOILM": "1",
    "FUGOLDTEN": "1",
    "OFCRUDEOILM": "1",
    "CPO": "1",
    "MCXBULLDEX": "1",
    
    # Stream 2 products
    "OFGOLDM": "2",
    "FUKAPAS": "2",
    "FUMENTHAOIL": "2",
    "FUCOTTONCNDY": "2",
    "FUCOTTONOIL": "2",
    "RUBBER": "2",
    "COTTON": "2",
    "COTTONREF": "2",
    "FUCARDAMOM": "2",
    "OFSILVER": "2",
    
    # Stream 3 products
    "OFSILVERM": "3",
    
    # Stream 4 products
    "FUSILVERMIC": "4",
    "FUCRUDEOIL": "4",
    "OFCRUDEOIL": "4",
    
    # Stream 5 products
    "FUSILVER": "5",
    "FUSILVERM": "5",
    "FUCOPPER": "5",
    "FUNICKEL": "5",
    "FUALUMINIUM": "5",
    "FUZINC": "5",
    "FULEAD": "5",
    "FUNATURALGAS": "5",
    "FUMCXMETLDEX": "5",
    "FUALUMINI": "5",
    "FUZINCMINI": "5",
    "FULEADMINI": "5",
    "FUSTEELREBAR": "5",
    "OFCOPPER": "5",
    "OFZINC": "5",
    "OFNATURALGAS": "5",
    "OFNATGASMINI": "5",
    "FUNATGASMINI": "5",
    "COPPERM": "5",
    "NICKELM": "5",
    "ALUMINI": "5",
    "MCXMETLDEX": "5",
    "FUELECDMBL": "5",
    "OGMCXBULLDEX": "5",
}

class MCXDataLoader:
    """Helper class for loading MCX data files"""
    
    @staticmethod
    def make_unique_columns(columns):
        counts = {}
        new_columns = []
        for col in columns:
            if col in counts:
                counts[col] += 1
                new_columns.append(f"{col}_{counts[col]}")
            else:
                counts[col] = 0
                new_columns.append(col)
        return new_columns

    # Column names from MCX Reference Data Files (Masters) doc (MCXScrips.bcp)
    field_names_scrips = [
        "Filler2", "Filler4", "Instrument PartitionID", "Filler8", "Filler2", 
        "Instrument Identifier", "Symbol", "Instrument Series", "Instrument Type", 
        "Permit Trading", "Filler4_2", "ProductID", "Bandhani Range", "Filler1_1", 
        "Filler1_2", "Filler1_3", "Filler1_4", "Instrument Start Date", "Filler4_3", 
        "Last Trading Date", "Lot Size", "Tick Size", "Instrument Description", 
        "CapacityGroupID", "Filler4_4", "Filler4_5", "Delivery Start Date", 
        "Delivery End Date", "Filler1_5", "Trade2Trade Indicator", "Index Flag", 
        "Default Index", "Index Instrument", "Feed Flag", "Filler1_6", "Filler1_7", 
        "Filler1_8", "Last Modified Date", "Instrument Status flag", "Instrument Info", 
        "Minimum Lot", "Tender Period Start Date", "Tender Period End Date", 
        "U/L Asset Group", "Name of Underlying U/L Asset", "Identifier of the underlying", 
        "Filler4_6", "Filler4_7", "Filler1_9", "Filler1_10", "Filler1_11", 
        "Filler1_12", "Filler1_13", "Instrument Name", "Original Expiry Date", 
        "Strike price", "Option Type", "CA level", "Segment ID", 
        "Additional Lean Period Margin", "Filler2_2", "Price quote unit", 
        "Price Quote quantity", "Terms of daily price range", "Upper Daily price range", 
        "Lower Daily price range", "Tender Period Indicator", "Settlement method", 
        "Terms of Initial Margin", "Buy Initial margin rate", "Base Price", 
        "Maximum single transaction quantity", "Maximum single transaction value", 
        "Instrument class", "Near month instrument identifier", "Far month instrument identifier", 
        "Trading unit", "Trading unit factor", "Delivery Unit", "Delivery unit factor", 
        "Price Numerator", "Specification", "Price denominator", "General Numerator", 
        "General denominator", "Lot Numerator", "Lot Denominator", "Decimal Locator", 
        "Filler2_3", "Filler15", "Filler4_8", "Filler50", "Additional Lean Period Margin (Sell)", 
        "Spread Benefit on Additional Lean Period Margin", "Sell Initial margin rate", 
        "ProductName", "Filler2_4", "Terms of special margin", "Buy Special Margin Rate", 
        "Sell Special Margin Rate", "Initial Margin Spread Benefit Flag", 
        "Instrument End Date Time", "Trading Currency", "Filler3", "Product Month", 
        "Pre Open Allowed", "Group Id", "Matching Type", "Spread Type", "Filler16", 
        "Value Method", "Additional Lean Period Margin (Buy)", "SLBM Eligibility", 
        "Terms of Extreme Loss Margin", "Buy Extreme Loss Margin Rate", "Sell Extreme Loss", 
        "Options Pricing Model", "Delivery Mode",
    ]

    # Column names for MCX_ProductMaster.csv
    product_master_headers = [
        "Instrument Name", "Instrument ID", "Unique Identifier", "Underlying Unique Identifier", 
        "Symbol", "Underlying Asset", "Underlying Group", "Options Type", "Strike Price", 
        "Expiry Date", "Base Price", "Product Description", "Quotation Quantity", 
        "Quotation Unit", "Unique ID Auction Buy In", "Unique ID Auction Sell Out", 
        "T2T Allowed", "Reserved", "Tradable Lot", "Price Tick", "Near Month Product Symbol", 
        "Far Month Product Symbol", "Product Start Date Time", "Product End Date Time", 
        "Tender Start Date Time", "Tender End Date", "Delivery Start Date", 
        "Delivery End Date", "Expiry Process Date", "Margin Indicator", "Regular Buy Margin", 
        "Regular Sell Margin", "Special Buy Margin", "Special Sell Margin", "Tender Buy Margin", 
        "Tender Sell Margin", "Delivery Buy Margin", "Delivery Sell Margin", "Limit for All Client", 
        "Limit for Only All Client", "Limit for Only All Own", "Limit Per Client Account", 
        "Limit Per Own Account", "Spread Benefit Allowed", "Record Deleted", "Remarks", 
        "Price Numerator", "Price Denominator", "General Numerator", "General Denominator", 
        "Lot Numerator", "Lot Denominator", "Decimal Locator", "Block Deal", "Currency Code", 
        "Reserved2", "Delivery Weight", "Delivery Unit", "Product Month", "Trade Group ID", 
        "Matching No", "Pre-Open Session", "Spread Type", "Extreme Loss Buy Margin", 
        "Extreme Loss Sell Margin", "Option Pricing Method", "Threshold Limit", "Delivery Mode"
    ]

    @staticmethod
    def load_mcx_scrips(base_dir, trade_date):
        ctcl_dir = os.path.join(base_dir, trade_date)
        
        # First check if JSON already exists
        json_path = os.path.join(ctcl_dir, "MCXScrips.json")
        if os.path.exists(json_path):
            # Load from existing JSON
            print(f"Loading scrips from existing JSON: {json_path}")
            with open(json_path, "r") as fp:
                scrips_dict = json.load(fp)
            print(f"MCXScrips loaded from JSON with {len(scrips_dict)} rows")
            
            # Verify ProductName field exists in JSON data
            sample_key = next(iter(scrips_dict.keys())) if scrips_dict else None
            if sample_key and "ProductName" in scrips_dict[sample_key]:
                product_names_with_data = sum(1 for v in scrips_dict.values() 
                                            if isinstance(v, dict) and v.get("ProductName") and str(v.get("ProductName")).strip())
                print(f"ProductName field verified in JSON: {product_names_with_data} rows have non-empty ProductName")
            else:
                print(f"WARNING: ProductName field not found in JSON sample. Check field_names_scrips mapping.")
            
            return scrips_dict
        
        scrips_path = os.path.join(ctcl_dir, "MCXScrips.bcp")

        if not os.path.exists(scrips_path):
            raise FileNotFoundError(f"{scrips_path} not found")
        
        print(f"Loading scrips from {scrips_path}")
        df = pd.read_csv(scrips_path, delimiter=",", header=None, dtype=str)

        df.columns = MCXDataLoader.make_unique_columns(MCXDataLoader.field_names_scrips[: len(df.columns)])
        scrips_dict = df.to_dict("index")

        # Verify ProductName column exists before saving JSON
        if "ProductName" in df.columns:
            product_names_with_data = df[df["ProductName"].notna() & (df["ProductName"].astype(str).str.strip() != "")]["ProductName"].nunique()
            print(f"ProductName column found: {product_names_with_data} unique non-empty ProductNames in {len(df)} rows")
        else:
            print(f"WARNING: ProductName column not found in loaded BCP file. Check field_names_scrips mapping.")

        with open(json_path, "w") as fp:
            json.dump(scrips_dict, fp, indent=4)

        print(f"MCXScrips loaded with {len(df)} rows, JSON saved at {json_path}")
        return scrips_dict

    @staticmethod
    def load_product_master(base_dir, trade_date):
        """Load MCX_ProductMaster.csv - simplified without multiplier calculations"""
        ctcl_dir = os.path.join(base_dir, trade_date)
        product_master_path = os.path.join(ctcl_dir, "MCX_ProductMaster.csv")

        if not os.path.exists(product_master_path):
            raise FileNotFoundError(f"{product_master_path} not found")
        
        print(f"Loading product master from {product_master_path}")
        df = pd.read_csv(product_master_path, header=None, names=MCXDataLoader.product_master_headers, dtype=str)
        
        num_cols = ["Price Numerator", "Price Denominator", "General Numerator", "General Denominator" ,"Quotation Quantity"]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["Quotation Unit"] = df["Quotation Unit"].fillna("")
        product_master_dict = df.to_dict("index")
        
        out_path = os.path.join(ctcl_dir, "MCX_ProductMaster.json")
        with open(out_path, "w") as fp:
            json.dump(product_master_dict, fp, indent=4)

        print(f"Product Master loaded with {len(df)} rows, JSON saved at {out_path}")
        return product_master_dict

    @staticmethod
    def bhavcopy_csv_to_json(file_path):
        bhav_df = pd.read_csv(file_path)
        bhav_dict = bhav_df.to_dict(orient='records')

        out_path = file_path.replace('.csv', '.json')
        with open(out_path, "w") as fp:
            json.dump(bhav_dict, fp, indent=4)
        print(f"bhavcopy loaded with {len(bhav_df)} rows, JSON saved at {out_path}")

        return bhav_dict

class MCXFileDownloader:
    """Class for downloading files from MCX SFTP"""
    
    def __init__(self, trade_date):
        self.trade_date = trade_date
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        })
        self.csrf_token = None
        self.session.verify = True
        
    def login(self):
        """Login to MCX SFTP and get CSRF token"""
        login_url = f"{mcx_config.BASE_URL}/Web Client/Login.xml?Command=Login"
        payload = {
            "user": mcx_config.USER,
            "pword": mcx_config.PASSWORD,
            "viewshare": "",
            "language": "en,US"
        }
        
        r = self.session.post(login_url, data=payload, verify=True)
        if "CsrfToken" not in r.text:
            raise Exception("Login failed")
            
        import re
        m = re.search(r"<CsrfToken>([^<]+)</CsrfToken>", r.text)
        if not m:
            raise Exception("Could not fetch CsrfToken")
            
        self.csrf_token = m.group(1)
        print("Successfully logged in and fetched CSRF token")
        return True
    
    def download_file(self, remote_path, remote_file, local_filename):
        """Download a file from MCX SFTP"""
        if not self.csrf_token:
            self.login()
        
        remote_file = unquote(remote_file)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
            
        encoded_path = quote(f"{remote_path}/{remote_file}", safe="/")  
        download_url = (
            f"{mcx_config.BASE_URL}/?Command=Download"
            f"&File={encoded_path}"
            f"&CsrfToken={self.csrf_token}"
        )
        
        print(f"Downloading {remote_path}/{remote_file} to {local_filename}")
        r = self.session.get(download_url,verify=True)
        
        if b"Serv-U - Error Occurred" in r.content:
            raise Exception(f"Download failed for {remote_file}")
        else:
            with open(local_filename, "wb") as f:
                f.write(r.content)
            print(f"Downloaded {remote_file} ({len(r.content)} bytes)")
            return True
    
    def download_latest_file(self, file_type: str, trade_date: str):
        if file_type.lower() == "index":
            trade_date_obj = datetime.strptime(trade_date, "%Y%m%d")
            previous_day_obj = trade_date_obj - timedelta(days=1)
            previous_day = previous_day_obj.strftime("%Y%m%d")
        
            year = previous_day[:4]
            month_num = int(previous_day[4:6])
            day = previous_day[6:8]

            month_str = month_abbr[month_num]
            remote_dir = f"/Common/Daily Index File/{year}/{month_str}"
        
            remote_file = f"DailyIndexFile{day}{month_num:02d}{year}.csv"
        
            local_filename = remote_file
            local_dir = mcx_config.LOCAL_DIRS["raw_files"] / trade_date

            os.makedirs(local_dir, exist_ok=True)
            local_path = local_dir / local_filename

            try:
                self.download_file(remote_dir, remote_file, str(local_path))
                print(f"{file_type.capitalize()} file downloaded: {local_path}")
                return True
            except Exception as e:
                print(f"{file_type.capitalize()} file not present for {trade_date} (tried previous day {previous_day}), skipping. ({e})")
                return False

        else:
            raise ValueError("file_type must be 'index'")


def download_mcx_files():
    """
    Download all required MCX files for today's date.
    
    Raises:
        FileNotFoundError: If CTCL directory cannot be created
        Exception: If download fails
    """
    import logging
    
    # Always use today's date
    trade_date = datetime.now().strftime("%Y%m%d")
    logging.info(f"Starting MCX file download for date: {trade_date}")
    
    # Get CTCL directory for the trade date
    ctcl_dir = mcx_config.get_ctcl_dir(trade_date)
    
    # Ensure directory exists
    ctcl_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"CTCL directory: {ctcl_dir}")
    
    # Initialize downloader
    downloader = MCXFileDownloader(trade_date)
    
    try:
        # Login to MCX SFTP
        logging.info("Logging in to MCX SFTP...")
        downloader.login()
        
        # Download contract master files
        logging.info("Downloading contract master files...")
        contract_files = mcx_config.get_contract_files()
        
        for file in contract_files:
            local_path = ctcl_dir / file
            remote_path = mcx_config.REMOTE_PATHS["contract_master"]
            
            # Product master is in a different remote path
            if file == "MCX_ProductMaster.csv":
                remote_path = mcx_config.REMOTE_PATHS["product_master"]
            
            logging.info(f"Downloading {file}...")
            downloader.download_file(remote_path, file, str(local_path))
            logging.info(f"✓ Downloaded {file}")
        
        # Download bhavcopy
        logging.info("Downloading bhavcopy...")
        bhavcopy_path = mcx_config.get_bhavcopy_path(trade_date)
        downloader.download_file(
            mcx_config.REMOTE_PATHS["bhavcopy"], 
            "Bhavcopy.csv", 
            str(bhavcopy_path)
        )
        logging.info(f"✓ Downloaded bhavcopy to {bhavcopy_path}")
        
        # Download index file (optional, may not exist for all dates)
        logging.info("Downloading index file (if available)...")
        try:
            downloader.download_latest_file("index", trade_date)
            logging.info("✓ Downloaded index file")
        except Exception as e:
            logging.warning(f"Index file not available: {e}")
        
        logging.info("=" * 80)
        logging.info("All required files downloaded successfully!")
        logging.info(f"Files location: {ctcl_dir}")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"Error downloading files: {e}")
        raise


def verify_mcx_files_exist() -> bool:
    """
    Verify that all required files exist locally for today's date.
    
    Returns:
        True if all files exist, False otherwise
    """
    import logging
    
    # Always use today's date
    trade_date = datetime.now().strftime("%Y%m%d")
    
    ctcl_dir = mcx_config.get_ctcl_dir(trade_date)
    bhavcopy_path = mcx_config.get_bhavcopy_path(trade_date)
    
    required_files = [
        ctcl_dir / "MCXScrips.bcp",
        ctcl_dir / "MCX_ASSET_MASTER.bcp",
        ctcl_dir / "MCX_PART.bcp",
        ctcl_dir / "MCX_ProductMaster.csv",
        bhavcopy_path,
    ]
    
    missing_files = []
    for file_path in required_files:
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    # Check for index file (optional - may not exist for all dates)
    # Index file is named based on previous day
    try:
        trade_date_obj = datetime.strptime(trade_date, "%Y%m%d")
        previous_day_obj = trade_date_obj - timedelta(days=1)
        previous_day = previous_day_obj.strftime("%Y%m%d")
        
        year = previous_day[:4]
        month_num = int(previous_day[4:6])
        day = previous_day[6:8]
        
        month_str = month_abbr[month_num]
        index_filename = f"DailyIndexFile{day}{month_num:02d}{year}.csv"
        index_file_path = ctcl_dir / index_filename
        
        if index_file_path.exists():
            logging.info(f"✓ Index file found: {index_filename}")
        else:
            logging.debug(f"Index file not found (optional): {index_filename}")
    except Exception as e:
        logging.debug(f"Could not check index file: {e}")
    
    if missing_files:
        logging.warning("Missing required files:")
        for file in missing_files:
            logging.warning(f"  - {file}")
        return False
    
    logging.info("✓ All required files are present")
    return True


if __name__ == "__main__":
    """
    MCX Files Downloader
    
    This script downloads all required files from MCX SFTP for refdata generation.
    It always uses today's date for downloading and verification.
    It should be run before make_refdata_mcx.py to ensure all files are present locally.
    After downloading, it automatically verifies that all files exist.
    
    Usage:
        python mcx_refdata_helper.py    # Download files for today
    """
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    
    today = datetime.now().strftime("%Y%m%d")
    logging.info(f"Processing date: {today}")
    
    # Download files (always uses today's date)
    try:
        download_mcx_files()
        
        # Verify after download (always uses today's date)
        if verify_mcx_files_exist():
            logging.info("✓ Verification passed: All files downloaded successfully")
        else:
            logging.error("✗ Verification failed: Some files are still missing")
            exit(1)
            
    except Exception as e:
        logging.error(f"Download failed: {e}")
        exit(1)
