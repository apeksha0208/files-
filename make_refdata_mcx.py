#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime
from mcx_refdata_helper import (
    mcx_config,
    MCXDataLoader,
    PRODUCT_NAME_TO_STREAM_MAP,
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

mcx_weekly_expiry_assets = [""]


class RefDataCreatorMCX:
    # Class-level constants
    INSTRUMENT_TYPE_MAP = {
        "1": "UNDERLYING",
        "2": "SPOT",
        "3": "OPT",
        "4": "FUT",
        "5": "AUCTION",
    }
    CATEGORY_MAP = {
        "OPTFUT": "COM_FO",
        "FUTCOM": "COM_FO",
        "FUTIDX": "COM_INDEX_FO",
        "OPTIDX": "COM_INDEX_FO",
        "COM": "COM",
    }
    REFDATA_COLUMNS = [
        "ZANSKAR_ID",
        "TOKEN",
        "PRODUCT_ID",
        "SETTLEMENT_GROUP",
        "SECURITY_TYPE",
        "CATEGORY",
        "EXCH_TICKER",
        "ZANSKAR_NAME",
        "ASSETCODE",
        "DERIVATIVE_TYPE",
        "EXPIRY_DATE",
        "EXPIRY_EXCHANGE",
        "EXPIRY_TAG",
        "UNDERLYING_ASSET",
        "STREAM_ID",
        "STRIKE_PRICE",
        "LOT_SIZE",
        "TICK_SIZE",
        "HIGH_PRICE_RANGE",
        "LOW_PRICE_RANGE",
        "PREV_HIGH",
        "PREV_LOW",
        "PREV_CLOSE",
        "PREV_LAST",
        "PREV_SETTLEMENT",
        "PREV_OPEN_INTEREST",
        "UNDERLYING_PREV_CLOSE",
        "UNDERLYING_INSTRUMENT",
        "ISIN",
        "FREEZE_QTY_LIMIT",
        "PRICE_NUMERATOR",
        "PRICE_DENOMINATOR",
        "GENERAL_NUMERATOR",
        "GENERAL_DENOMINATOR",
        "QUOTATION_QUANTITY",
        "QUOTATION_UNIT",
    ]

    def __init__(self, trade_date):
        """
        Initialize RefDataCreatorMCX for a given trade date.
        
        Args:
            trade_date: Trade date in YYYYMMDD format
            
        Raises:
            FileNotFoundError: If required files are not present locally.
                              Use download_mcx_files.py to download files first.
        """
        self.date = trade_date
        self.trade_date = trade_date
        self.ctcl_dir = mcx_config.get_ctcl_dir(trade_date)

        # Verify required files exist
        if not os.path.exists(self.ctcl_dir):
            raise FileNotFoundError(
                f"CTCL dir not found: {self.ctcl_dir}\n"
                f"Please run mcx_refdata_helper.py first to download required files."
            )
        
        # Verify critical files exist
        required_files = [
            self.ctcl_dir / "MCXScrips.bcp",
            self.ctcl_dir / "MCX_ProductMaster.csv",
            mcx_config.get_bhavcopy_path(trade_date),
        ]
        
        missing_files = [str(f) for f in required_files if not f.exists()]
        if missing_files:
            raise FileNotFoundError(
                f"Required files not found:\n" + "\n".join(f"  - {f}" for f in missing_files) +
                f"\n\nPlease run mcx_refdata_helper.py first to download required files."
            )

        self.bhavPath = mcx_config.get_bhavcopy_path(trade_date)
        self.zanskar_id = 0

        # Use list for better performance, convert to DataFrame at end
        self.refdata_rows = []

        # Load and prepare data
        scrips = MCXDataLoader.load_mcx_scrips(
            str(mcx_config.LOCAL_DIRS["raw_files"]), self.date
        )
        self.scrips_df = pd.DataFrame.from_dict(scrips, orient="index")

        # Optimize scrips dataframe
        self.scrips_df["Symbol"] = self.scrips_df["Symbol"].str.strip()
        self.scrips_df["ProductID"] = (
            self.scrips_df["ProductID"].astype(str).str.strip()
        )
        self.scrips_df["Instrument Type"] = self.scrips_df["Instrument Type"].astype(
            str
        )
        self.scrips_df["Instrument Identifier"] = (
            self.scrips_df["Instrument Identifier"].astype(str).str.strip()
        )
        # Normalize ProductName for stream ID lookup (handle NaN/None)
        # ProductName comes from MCXScrips.json (or .bcp) and must be normalized
        if "ProductName" in self.scrips_df.columns:
            self.scrips_df["ProductName"] = self.scrips_df["ProductName"].fillna("").astype(str).str.strip()
            # Log ProductName availability for verification
            non_empty_product_names = self.scrips_df[self.scrips_df["ProductName"] != ""]["ProductName"].nunique()
            logging.info(f"ProductName field found: {len(self.scrips_df)} rows, {non_empty_product_names} unique non-empty ProductNames")
        else:
            logging.warning("ProductName column not found in scrips dataframe - stream ID assignment will fail")

        # Filter early: skip ProductID == "0" or Instrument Type == "1"
        self.scrips_df = self.scrips_df[
            (self.scrips_df["ProductID"] != "0")
            & (self.scrips_df["Instrument Type"] != "1")
        ].copy()

        product_master = MCXDataLoader.load_product_master(
            str(mcx_config.LOCAL_DIRS["raw_files"]), self.date
        )
        self.product_master_df = pd.DataFrame.from_dict(product_master, orient="index")

        # Build product master lookup dict for O(1) access
        self.product_master_df["Unique Identifier"] = (
            self.product_master_df["Unique Identifier"].astype(str).str.strip()
        )
        self.product_master_map = self.product_master_df.set_index(
            "Unique Identifier"
        ).to_dict("index")

        self.bhavcopy_df = pd.DataFrame()
        # Only PRODUCT_NAME_TO_STREAM_MAP is used for stream ID 
        self.product_name_to_stream_map = PRODUCT_NAME_TO_STREAM_MAP

    def get_stream_id(self, product_name):
        """
        Get stream ID based on ProductName only.
        
        Args:
            product_name: ProductName from scrip data
        
        Returns:
            Stream ID as string (e.g., "1", "2", "3", "4", "5")
        
        Raises:
            KeyError: If ProductName is not found in PRODUCT_NAME_TO_STREAM_MAP
        """
        product_name_clean = str(product_name).strip().upper()
        if product_name_clean in self.product_name_to_stream_map:
            stream_id = self.product_name_to_stream_map[product_name_clean]
            logging.debug(f"Stream ID from ProductName '{product_name_clean}': {stream_id}")
            return stream_id
        
        # Raise error if ProductName not found
        error_msg = (
            f"STREAM CONFIG ERROR: ProductName '{product_name_clean}' not found in PRODUCT_NAME_TO_STREAM_MAP. "
            f"Please add this ProductName to the mapping."
        )
        raise KeyError(error_msg)

    def get_product_master_fields(self, instrument_id):
        """Get the six fields from product master - optimized with dict lookup"""
        instrument_id_str = str(instrument_id).strip()

        match = self.product_master_map.get(instrument_id_str)
        if match is None:
            raise ValueError(
                f"Instrument ID {instrument_id_str} not found in Product Master"
            )

        return (
            float(match["Price Numerator"]),
            float(match["Price Denominator"]),
            float(match["General Numerator"]),
            float(match["General Denominator"]),
            int(match["Quotation Quantity"]),
            str(match.get("Quotation Unit", "")),
        )

    @staticmethod
    def get_deriv_type(inst_type, op_type):
        """Static method for derivative type calculation"""
        deriv = RefDataCreatorMCX.INSTRUMENT_TYPE_MAP[inst_type]
        if inst_type == "3":
            return f"{deriv}_{op_type}"
        return deriv

    @staticmethod
    def epoch_to_date(epoch_val):
        """Static method for epoch conversion"""
        if epoch_val == "0":
            return "N/A"
        return datetime.utcfromtimestamp(int(epoch_val)).strftime("%Y%m%d")

    def load_bhavcopy(self):
        """Load and optimize bhavcopy dataframe"""
        if not os.path.exists(self.bhavPath):
            raise FileNotFoundError(f"{self.bhavPath} not found")

        logging.info(f"Loading BhavCopy from {self.bhavPath}")
        self.bhavcopy_df = pd.read_csv(self.bhavPath, dtype=str)

        # Optimize: set index for O(1) lookups
        self.bhavcopy_df["INSTRUMENTID"] = self.bhavcopy_df["INSTRUMENTID"].str.strip()
        self.bhavcopy_df = self.bhavcopy_df.set_index("INSTRUMENTID")

        logging.info(f"Bhavcopy loaded: {len(self.bhavcopy_df)} rows")

    def get_bhav_value(self, instrument_id, column):
        """Optimized bhav value retrieval using index.
        Returns 0 if instrument not in bhavcopy (untraded contract) or value is NaN.
        """
        try:
            val = self.bhavcopy_df.loc[str(instrument_id).strip(), column]
            return 0 if pd.isna(val) else val
        except KeyError:
            return 0

    def _get_next_nearest_future_close(self, asset, option_expiry_date, instrument_id):
        """
        Get close price of future with next nearest expiry for a given option.

        Args:
            asset: Asset symbol (e.g., "CRUDEOIL")
            option_expiry_date: Option's expiry date in YYYYMMDD format
            instrument_id: Option's instrument ID for error messages

        Returns:
            Tuple of (close_price: float, future_zanskar_name: str)

        Raises:
            ValueError: If no future found with expiry >= option expiry
        """
        # Edge Case 1: Asset not in future_price_map_by_date
        if asset not in self.future_price_map_by_date:
            error_msg = (
                f"CRITICAL: No futures found for asset '{asset}' "
                f"for option Instrument ID {instrument_id} (expiry={option_expiry_date}). "
                f"Available assets: {list(self.future_price_map_by_date.keys())}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)

        # Edge Case 2: Empty futures list for asset
        futures_list = self.future_price_map_by_date[asset]
        if len(futures_list) == 0:
            error_msg = (
                f"CRITICAL: Empty futures list for asset '{asset}' "
                f"for option Instrument ID {instrument_id} (expiry={option_expiry_date})"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)

        # Convert option expiry to datetime for comparison
        try:
            option_expiry_dt = datetime.strptime(option_expiry_date, "%Y%m%d")
        except ValueError as e:
            error_msg = (
                f"CRITICAL: Invalid option expiry date format '{option_expiry_date}' "
                f"for Instrument ID {instrument_id}. Expected YYYYMMDD format."
            )
            logging.error(error_msg)
            raise ValueError(error_msg) from e

        # Find first future with expiry_date > option_expiry_date (next nearest)
        same_expiry_future_close = None
        same_expiry_future_name = None

        for fut_expiry_date, fut_close in futures_list:
            try:
                fut_expiry_dt = datetime.strptime(fut_expiry_date, "%Y%m%d")
            except ValueError as e:
                error_msg = (
                    f"CRITICAL: Invalid future expiry date format '{fut_expiry_date}' "
                    f"for asset '{asset}'. Expected YYYYMMDD format."
                )
                logging.error(error_msg)
                raise ValueError(error_msg) from e

            fut_zanskar_name = f"FUT_{asset}_{fut_expiry_date}"

            # Track same expiry future (fallback if no next nearest exists)
            if fut_expiry_dt == option_expiry_dt:
                same_expiry_future_close = fut_close
                same_expiry_future_name = fut_zanskar_name

            # Return first future with expiry after option expiry (next nearest)
            if fut_expiry_dt > option_expiry_dt:
                logging.debug(
                    f"Found next nearest future for {asset}: "
                    f"option_expiry={option_expiry_date}, "
                    f"future_expiry={fut_expiry_date}, "
                    f"close_price={fut_close:.2f}"
                )
                return fut_close, fut_zanskar_name

        # Edge Case 3: No future with expiry > option expiry found
        # Fallback: Use same expiry future if available
        if same_expiry_future_close is not None:
            logging.debug(
                f"Using same expiry future for {asset}: "
                f"option_expiry={option_expiry_date}, "
                f"future_expiry={option_expiry_date}, "
                f"close_price={same_expiry_future_close:.2f} "
                f"(no future with expiry > option expiry found)"
            )
            return same_expiry_future_close, same_expiry_future_name

        # If no same expiry future exists, raise error
        all_fut_expiries = [f[0] for f in futures_list]
        error_msg = (
            f"CRITICAL: No future with expiry > {option_expiry_date} or expiry == {option_expiry_date} "
            f"found for asset '{asset}' for option Instrument ID {instrument_id}. "
            f"Available future expiries: {all_fut_expiries}"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)

    def build_future_price_map(self):
        """
        Build future price map organized by asset and expiry_date.

        Creates map:
        future_price_map_by_date: {asset: [(expiry_date, close_price), ...]} - sorted by expiry

        Untraded contracts (not in bhavcopy, close=0) are excluded from the map
        because a 0 price is meaningless as an underlying reference for options.
        They are still included in refdata output with all bhav fields as 0.
        """
        self.future_price_map_by_date = {}

        # Filter futures only
        futures_df = self.scrips_df[self.scrips_df["Instrument Type"] == "4"].copy()

        if len(futures_df) == 0:
            logging.warning("No futures found in scrips data")

        for _, scrip in futures_df.iterrows():
            asset = scrip["Symbol"]
            expiry_date = self.epoch_to_date(scrip["Original Expiry Date"])
            fut_id = scrip["Instrument Identifier"]

            fut_close_raw = self.get_bhav_value(fut_id, "CLOSINGPRICE")

            # Untraded contract - present in scrips/master but not in bhavcopy (no trading yet)
            # Exclude from future price map only - 0 price not useful for options underlying lookup
            # The contract itself will still appear in refdata output with bhav fields as 0
            if fut_close_raw == 0 or pd.isna(fut_close_raw):
                logging.warning(
                    f"Excluding from future price map (untraded contract): ID={fut_id}, "
                    f"asset={asset}, expiry={expiry_date} (close=0, not in bhavcopy)"
                )
                continue

            fut_close = float(fut_close_raw) / 1e6

            if fut_close <= 0:
                error_msg = (
                    f"CRITICAL: Future close price is <= 0 for "
                    f"Instrument ID {fut_id}, asset={asset}, expiry_date={expiry_date}, close={fut_close}"
                )
                logging.error(error_msg)
                raise ValueError(error_msg)

            # Store by asset and expiry_date for next nearest lookup
            if asset not in self.future_price_map_by_date:
                self.future_price_map_by_date[asset] = []
            self.future_price_map_by_date[asset].append((expiry_date, fut_close))

        # Sort futures by expiry_date for each asset (ascending order)
        for asset in self.future_price_map_by_date:
            if len(self.future_price_map_by_date[asset]) == 0:
                error_msg = f"CRITICAL: Empty futures list for asset {asset}"
                logging.error(error_msg)
                raise ValueError(error_msg)

            # Sort by expiry_date
            self.future_price_map_by_date[asset].sort(key=lambda x: x[0])

            # Check for duplicate expiry dates (shouldn't happen but validate)
            expiry_dates = [x[0] for x in self.future_price_map_by_date[asset]]
            if len(expiry_dates) != len(set(expiry_dates)):
                duplicates = [d for d in expiry_dates if expiry_dates.count(d) > 1]
                error_msg = (
                    f"CRITICAL: Duplicate expiry dates found for asset {asset}: {set(duplicates)}"
                )
                logging.error(error_msg)
                raise ValueError(error_msg)

        logging.info(
            f"Built future price map: {len(self.future_price_map_by_date)} assets"
        )

    def build_expiry_tags(self):
        """Build expiry tags mapping"""
        asset_expiry_dict = {}

        # Group by asset and derivative type
        for _, scrip in self.scrips_df.iterrows():
            asset = scrip["Symbol"]
            deriv_type = self.INSTRUMENT_TYPE_MAP[scrip["Instrument Type"]]
            group = f"{asset}.{deriv_type}"
            expiry_date = self.epoch_to_date(scrip["Original Expiry Date"])
            asset_expiry_dict.setdefault(group, set()).add(expiry_date)

        # Build expiry tag map
        expiry_map = {}
        for group, expiries in asset_expiry_dict.items():
            asset, _ = group.split(".")
            sorted_expiries = sorted(expiries)
            prefix = "W" if asset in mcx_weekly_expiry_assets else "M"

            for idx, exp in enumerate(sorted_expiries):
                expiry_map[(group, exp)] = f"{prefix}.{idx}"

        self.expiry_tag_map = expiry_map

    def process_scrip(self, scrip):
        """Process single scrip and return row data.

        If instrument is present in scrips/master but missing from bhavcopy,
        all bhav fields (prev_close, prev_high, prev_low, prev_oi, underlying_prev_close)
        will be 0. This handles untraded contracts - listed by MCX but never traded yet.
        """
        asset = scrip["Symbol"]
        inst_type = scrip["Instrument Type"]
        derivative_type = self.get_deriv_type(inst_type, scrip["Option Type"])
        instrument_id = scrip["Instrument Identifier"]
        deriv_type = self.INSTRUMENT_TYPE_MAP[inst_type]

        # Expiry handling
        expiry_date = (
            self.epoch_to_date(scrip["Original Expiry Date"])
            if deriv_type in ["OPT", "FUT"]
            else "N/A"
        )
        if expiry_date and expiry_date != "N/A":
            # Use Original Expiry Date for expiry_exch (keep in epoch format)
            expiry_exch = scrip["Original Expiry Date"]

            # Check if expiry date is in the past - CRITICAL ERROR
            date_obj = datetime.strptime(expiry_date, "%Y%m%d")
            trade_date_obj = datetime.strptime(self.trade_date, "%Y%m%d")
            if date_obj < trade_date_obj:
                error_msg = f"CRITICAL: Expiry date {expiry_date} is earlier than trade date {self.trade_date} for Instrument ID {instrument_id}"
                logging.error(error_msg)
                raise RuntimeError(error_msg)
        else:
            expiry_exch = 0

        # Get product master fields
        price_num, price_denom, gen_num, gen_denom, quot_qty, quot_unit = (
            self.get_product_master_fields(instrument_id)
        )

        # Bhav values - returns 0 naturally if instrument not in bhavcopy (untraded contract)
        prev_oi = int(self.get_bhav_value(instrument_id, "OPENINTEREST"))
        prev_low = int(self.get_bhav_value(instrument_id, "LOWPRC")) / 1e6
        prev_high = int(self.get_bhav_value(instrument_id, "HIGHPRC")) / 1e6
        prev_close = int(self.get_bhav_value(instrument_id, "CLOSINGPRICE")) / 1e6

        # Expiry tag
        group = f"{asset}.{deriv_type}"
        expiry_tag = self.expiry_tag_map.get((group, expiry_date), "")

        # Zanskar name
        zanskar_name = (
            f"{deriv_type}_{asset}_{expiry_date}"
            if deriv_type in ["OPT", "FUT"]
            else asset
        )
        if derivative_type in ["OPT_CE", "OPT_PE"]:
            zanskar_name += f"_{scrip.get('Option Type')}_{scrip['Strike price']}"

        # Underlying prev close logic
        if derivative_type.startswith("OPT"):
            # For options: use future with next nearest expiry (expiry > option expiry)
            # Returns tuple (close_price, future_zanskar_name)
            underlying_prev_close, underly_instrument = self._get_next_nearest_future_close(
                asset, expiry_date, instrument_id
            )
        elif derivative_type == "FUT":
            # For futures: underlying close is same as future's own close
            # Untraded contracts not in bhavcopy will naturally have prev_close=0
            underlying_prev_close = prev_close
            underly_instrument = zanskar_name  # future is its own underlying
            if underlying_prev_close <= 0:
                logging.warning(
                    f"Untraded contract FUT: ID={instrument_id}, asset={asset}, "
                    f"expiry={expiry_date} - UNDERLYING_PREV_CLOSE=0 (not in bhavcopy)"
                )
                underlying_prev_close = 0
        else:
            # For other types (UNDERLYING, SPOT, etc.): use their own close price
            underlying_prev_close = prev_close
            underly_instrument = "N/A"

        # Category and other fields
        category = self.CATEGORY_MAP[scrip["Instrument Name"].strip()]
        underlying_asset = (
            scrip["Symbol"]
            if inst_type in ["3", "4"]
            else scrip["Name of Underlying U/L Asset"].replace(" ", "")
        )
        # Get stream ID using ProductName
        # ProductName comes from MCXScrips.json (loaded from .bcp file)
        # Access ProductName from scrip (pandas Series when iterating)
        product_name = scrip.get("ProductName", "")
        
        # Handle None, NaN, or empty string
        if product_name is None or pd.isna(product_name) or str(product_name).strip() == "":
            error_msg = (
                f"CRITICAL: ProductName is empty or missing for Instrument ID {instrument_id}, "
                f"Symbol '{asset}'. ProductName must be present in MCXScrips.json file."
            )
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        # Normalize ProductName (should already be normalized in __init__, but ensure here)
        product_name = str(product_name).strip()
        
        # Get stream ID from ProductName mapping
        stream_id = self.get_stream_id(product_name)
        
        # Log successful stream ID assignment for debugging
        logging.debug(f"Assigned Stream ID {stream_id} for ProductName '{product_name}' (Instrument ID {instrument_id})")

        lot_size_raw = scrip["Lot Size"]
        general_numerator = int(scrip["General Numerator"])
        general_denominator = int(scrip["General denominator"])

        if general_numerator != gen_num or general_denominator != gen_denom:
            error_msg = (
                f"Mismatch for Instrument ID {instrument_id}: PM Num/Denom ({gen_num}/{gen_denom}) vs Scrip Num/Denom ({general_numerator}/{general_denominator})"
            )
            logging.error(error_msg)
            raise ValueError("Product Master and Scrip General Num/Denom mismatch")

        if general_denominator == 0:
            error_msg = f"General Denominator is 0 for Instrument ID {instrument_id}"
            logging.error(error_msg)
            raise ValueError("General Denominator is 0")

        qty_multiplier = general_numerator / general_denominator
        lot_size = qty_multiplier * int(lot_size_raw)

        if lot_size <= 0 or not lot_size.is_integer():
            error_msg = f"Invalid lot size for Instrument ID {instrument_id} : {lot_size}={general_numerator}//{general_denominator}*{lot_size_raw}"
            logging.error(error_msg)
            raise ValueError("Invalid lot size")

        freeze_qty_raw = scrip["Maximum single transaction quantity"]
        
        # Get percentage values from scrip data
        high_price_range_percent = float(scrip["Upper Daily price range"])
        low_price_range_percent = float(scrip["Lower Daily price range"])
        
        # For futures: HIGH_PRICE_RANGE and LOW_PRICE_RANGE are percentages
        # Calculate as: UNDERLYING_PREV_CLOSE * (1 ± percentage/100)
        if derivative_type == "FUT":
            if underlying_prev_close > 0:
                # Calculate based on UNDERLYING_PREV_CLOSE
                high_price_range = underlying_prev_close * (1 + high_price_range_percent / 100)
                low_price_range = underlying_prev_close * (1 - low_price_range_percent / 100)
            else:
                # Untraded contract (underlying_prev_close = 0), use 0 for both
                high_price_range = 0
                low_price_range = 0
                logging.warning(
                    f"Untraded contract FUT: ID={instrument_id}, asset={asset}, "
                    f"expiry={expiry_date} - HIGH/LOW_PRICE_RANGE=0 (underlying_prev_close=0)"
                )
        else:
            # For options and other types: use percentage values as-is (absolute values)
            high_price_range = high_price_range_percent
            low_price_range = low_price_range_percent
        
        freeze_qty = int(lot_size) * int(freeze_qty_raw)

        return [
            self.zanskar_id,
            scrip["Instrument Identifier"],
            scrip["ProductID"].strip(),
            "N/A",
            "N/A",
            category,
            zanskar_name,
            zanskar_name,
            scrip["ProductName"].strip(),
            derivative_type,
            expiry_date,
            expiry_exch,
            expiry_tag,
            underlying_asset,
            stream_id,
            scrip["Strike price"],
            int(lot_size),
            scrip["Tick Size"],
            int(high_price_range),
            int(low_price_range),
            int(prev_high),
            int(prev_low),
            int(prev_close),
            int(prev_close),
            int(prev_close),
            prev_oi,
            int(underlying_prev_close),
            underly_instrument,
            "N/A",
            freeze_qty,
            price_num,
            price_denom,
            gen_num,
            gen_denom,
            quot_qty,
            quot_unit,
        ]

    def make_refdata(self):
        """Optimized refdata creation using vectorized operations where possible"""
        self.build_expiry_tags()
        self.build_future_price_map()

        processed_count = 0
        skipped_count = 0
        unique_underlying_close_prices = set()

        # Process all scrips (already filtered in __init__)
        for _, scrip in self.scrips_df.iterrows():
            try:
                row_data = self.process_scrip(scrip)
                self.refdata_rows.append(row_data)
                self.zanskar_id += 1
                processed_count += 1

                # Track unique underlying close prices (column index 26 = UNDERLYING_PREV_CLOSE)
                underlying_close = row_data[26]
                if underlying_close is not None and underlying_close != "N/A":
                    try:
                        unique_underlying_close_prices.add(float(underlying_close))
                    except (ValueError, TypeError):
                        logging.warning(
                            f"Non-numeric underlying close price for Instrument ID {scrip['Instrument Identifier']}: {underlying_close}"
                        )
            except RuntimeError:
                # Re-raise RuntimeError to fail the entire process
                raise
            except (ValueError, KeyError) as e:
                error_msg = f"CRITICAL: Skipping Instrument ID {scrip['Instrument Identifier']}: {e}"
                logging.error(error_msg)
                skipped_count += 1
                continue

        logging.info(
            f"RefData creation completed: {processed_count} processed, {skipped_count} skipped"
        )
        logging.info(
            f"Unique underlying close prices: {len(unique_underlying_close_prices)} unique values"
        )
        if unique_underlying_close_prices:
            sorted_prices = sorted(unique_underlying_close_prices)
            logging.info(
                f"Underlying close price range: min={sorted_prices[0]:.2f}, "
                f"max={sorted_prices[-1]:.2f}, "
                f"median={sorted_prices[len(sorted_prices)//2]:.2f}"
            )

    def save_refdata_csv(self):
        """Convert list to DataFrame and save"""
        self.final_refdata_data_frame = pd.DataFrame(
            self.refdata_rows, columns=self.REFDATA_COLUMNS
        )
        out_file = mcx_config.get_refdata_path(self.trade_date)
        self.final_refdata_data_frame.to_csv(out_file, index=False)
        logging.info(f"Refdata saved at: {out_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate MCX refdata from local files",
        epilog="Note: Files must be downloaded first using mcx_refdata_helper.py"
    )
    parser.add_argument(
        "-d", 
        type=str, 
        required=False,
        help="Trade date in YYYYMMDD format (default: today)"
    )

    args = parser.parse_args()

    if args.d:
        date = args.d
        logging.info(f"Using date arg: {date}. Make sure files are present locally.")
    else:
        date = datetime.now().strftime("%Y%m%d")
        logging.info(f"Using today's date: {date}. Make sure files are present locally.")

    logging.info(f"Processing date: {date}")
    logging.info("=" * 80)
    logging.info("MCX RefData Generation")
    logging.info("=" * 80)
    logging.info("Note: This script assumes files are already downloaded.")
    logging.info("      Use mcx_refdata_helper.py to download files if needed.")
    logging.info("=" * 80)

    try:
        creator = RefDataCreatorMCX(date)
        creator.load_bhavcopy()
        creator.make_refdata()
        creator.save_refdata_csv()
        logging.info("=" * 80)
        logging.info("RefData generation completed successfully!")
        logging.info("=" * 80)
    except FileNotFoundError as e:
        logging.error("=" * 80)
        logging.error("ERROR: Required files not found!")
        logging.error("=" * 80)
        logging.error(str(e))
        logging.error("\nTo download files, run:")
        logging.error(f"  python mcx_refdata_helper.py -d {date}")
        exit(1)
    except Exception as e:
        logging.error(f"Error generating refdata: {e}")
        raise