#!/usr/bin/env python3
"""
Trade Data Generator for CCR (Counterparty Credit Risk) Calculations

Generates synthetic trade data with 80+ trading products across multiple asset classes.
Designed to produce realistic test data for CCR, SA-CCR, and risk management systems.

Usage:
    python trade_generator.py [OPTIONS]
    
    Options:
        --count N       Number of trades to generate (default: 200000)
        --output FILE   Output file path (default: trades.csv)
        --delimiter D   CSV delimiter (default: |)
        --seed N        Random seed for reproducibility
        --format FMT    Output format: csv, json (default: csv)

Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com
"""

import random
import string
import csv
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum
import sys

# =============================================================================
# TRADING PRODUCTS CATALOG (80+ Products)
# =============================================================================

class AssetClass(Enum):
    """SA-CCR Asset Classes"""
    INTEREST_RATE = "InterestRate"
    FX = "ForeignExchange"
    CREDIT = "Credit"
    EQUITY = "Equity"
    COMMODITY = "Commodity"

# Product definitions with SA-CCR relevant metadata
PRODUCTS = {
    # =========================================================================
    # INTEREST RATE DERIVATIVES (25 products)
    # =========================================================================
    "IRS_FIXED_FLOAT": {"asset_class": "InterestRate", "sub_class": "Swap", "sa_ccr_category": "IR", "typical_tenor_years": (1, 30)},
    "IRS_FLOAT_FLOAT": {"asset_class": "InterestRate", "sub_class": "BasisSwap", "sa_ccr_category": "IR", "typical_tenor_years": (1, 20)},
    "IRS_OIS": {"asset_class": "InterestRate", "sub_class": "OIS", "sa_ccr_category": "IR", "typical_tenor_years": (0.25, 10)},
    "IRS_XCCY": {"asset_class": "InterestRate", "sub_class": "CrossCurrencySwap", "sa_ccr_category": "IR", "typical_tenor_years": (1, 15)},
    "IRS_INFLATION": {"asset_class": "InterestRate", "sub_class": "InflationSwap", "sa_ccr_category": "IR", "typical_tenor_years": (2, 30)},
    "IRS_ZCS": {"asset_class": "InterestRate", "sub_class": "ZeroCouponSwap", "sa_ccr_category": "IR", "typical_tenor_years": (1, 30)},
    "IRS_AMORTIZING": {"asset_class": "InterestRate", "sub_class": "AmortizingSwap", "sa_ccr_category": "IR", "typical_tenor_years": (3, 20)},
    "FRA": {"asset_class": "InterestRate", "sub_class": "FRA", "sa_ccr_category": "IR", "typical_tenor_years": (0.25, 2)},
    "CAP": {"asset_class": "InterestRate", "sub_class": "Cap", "sa_ccr_category": "IR", "typical_tenor_years": (1, 10)},
    "FLOOR": {"asset_class": "InterestRate", "sub_class": "Floor", "sa_ccr_category": "IR", "typical_tenor_years": (1, 10)},
    "COLLAR": {"asset_class": "InterestRate", "sub_class": "Collar", "sa_ccr_category": "IR", "typical_tenor_years": (1, 10)},
    "SWAPTION_PAYER": {"asset_class": "InterestRate", "sub_class": "Swaption", "sa_ccr_category": "IR", "typical_tenor_years": (0.5, 10)},
    "SWAPTION_RECEIVER": {"asset_class": "InterestRate", "sub_class": "Swaption", "sa_ccr_category": "IR", "typical_tenor_years": (0.5, 10)},
    "SWAPTION_STRADDLE": {"asset_class": "InterestRate", "sub_class": "Swaption", "sa_ccr_category": "IR", "typical_tenor_years": (0.5, 5)},
    "BOND_OPTION": {"asset_class": "InterestRate", "sub_class": "BondOption", "sa_ccr_category": "IR", "typical_tenor_years": (0.25, 5)},
    "BOND_FUTURE": {"asset_class": "InterestRate", "sub_class": "BondFuture", "sa_ccr_category": "IR", "typical_tenor_years": (0.25, 1)},
    "IR_FUTURE": {"asset_class": "InterestRate", "sub_class": "IRFuture", "sa_ccr_category": "IR", "typical_tenor_years": (0.25, 2)},
    "IR_FUTURE_OPTION": {"asset_class": "InterestRate", "sub_class": "IRFutureOption", "sa_ccr_category": "IR", "typical_tenor_years": (0.1, 1)},
    "REPO": {"asset_class": "InterestRate", "sub_class": "Repo", "sa_ccr_category": "IR", "typical_tenor_years": (0.01, 1)},
    "REVERSE_REPO": {"asset_class": "InterestRate", "sub_class": "ReverseRepo", "sa_ccr_category": "IR", "typical_tenor_years": (0.01, 1)},
    "BOND_FORWARD": {"asset_class": "InterestRate", "sub_class": "BondForward", "sa_ccr_category": "IR", "typical_tenor_years": (0.1, 2)},
    "CMS_SWAP": {"asset_class": "InterestRate", "sub_class": "CMSSwap", "sa_ccr_category": "IR", "typical_tenor_years": (2, 20)},
    "CMS_CAP": {"asset_class": "InterestRate", "sub_class": "CMSCap", "sa_ccr_category": "IR", "typical_tenor_years": (2, 15)},
    "CMS_FLOOR": {"asset_class": "InterestRate", "sub_class": "CMSFloor", "sa_ccr_category": "IR", "typical_tenor_years": (2, 15)},
    "CALLABLE_SWAP": {"asset_class": "InterestRate", "sub_class": "CallableSwap", "sa_ccr_category": "IR", "typical_tenor_years": (3, 15)},
    
    # =========================================================================
    # FX DERIVATIVES (18 products)
    # =========================================================================
    "FX_SPOT": {"asset_class": "ForeignExchange", "sub_class": "Spot", "sa_ccr_category": "FX", "typical_tenor_years": (0.01, 0.01)},
    "FX_FORWARD": {"asset_class": "ForeignExchange", "sub_class": "Forward", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 2)},
    "FX_NDF": {"asset_class": "ForeignExchange", "sub_class": "NDF", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 2)},
    "FX_SWAP": {"asset_class": "ForeignExchange", "sub_class": "FXSwap", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 5)},
    "FX_OPTION_VANILLA": {"asset_class": "ForeignExchange", "sub_class": "VanillaOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 3)},
    "FX_OPTION_BARRIER": {"asset_class": "ForeignExchange", "sub_class": "BarrierOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 2)},
    "FX_OPTION_DIGITAL": {"asset_class": "ForeignExchange", "sub_class": "DigitalOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_OPTION_ASIAN": {"asset_class": "ForeignExchange", "sub_class": "AsianOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.25, 1)},
    "FX_OPTION_LOOKBACK": {"asset_class": "ForeignExchange", "sub_class": "LookbackOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_OPTION_COMPOUND": {"asset_class": "ForeignExchange", "sub_class": "CompoundOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.25, 1)},
    "FX_OPTION_CHOOSER": {"asset_class": "ForeignExchange", "sub_class": "ChooserOption", "sa_ccr_category": "FX", "typical_tenor_years": (0.25, 1)},
    "FX_STRADDLE": {"asset_class": "ForeignExchange", "sub_class": "Straddle", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_STRANGLE": {"asset_class": "ForeignExchange", "sub_class": "Strangle", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_RISK_REVERSAL": {"asset_class": "ForeignExchange", "sub_class": "RiskReversal", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_BUTTERFLY": {"asset_class": "ForeignExchange", "sub_class": "Butterfly", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_SEAGULL": {"asset_class": "ForeignExchange", "sub_class": "Seagull", "sa_ccr_category": "FX", "typical_tenor_years": (0.1, 1)},
    "FX_TARGET_REDEMPTION": {"asset_class": "ForeignExchange", "sub_class": "TARF", "sa_ccr_category": "FX", "typical_tenor_years": (0.5, 2)},
    "FX_ACCUMULATOR": {"asset_class": "ForeignExchange", "sub_class": "Accumulator", "sa_ccr_category": "FX", "typical_tenor_years": (0.25, 1)},
    
    # =========================================================================
    # CREDIT DERIVATIVES (18 products)
    # =========================================================================
    "CDS_SINGLE_NAME": {"asset_class": "Credit", "sub_class": "SingleNameCDS", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 10)},
    "CDS_INDEX": {"asset_class": "Credit", "sub_class": "IndexCDS", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 10)},
    "CDS_INDEX_TRANCHE": {"asset_class": "Credit", "sub_class": "IndexTranche", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 7)},
    "CDX_IG": {"asset_class": "Credit", "sub_class": "CDX_IG", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 10)},
    "CDX_HY": {"asset_class": "Credit", "sub_class": "CDX_HY", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 5)},
    "ITRAXX_MAIN": {"asset_class": "Credit", "sub_class": "iTraxxMain", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 10)},
    "ITRAXX_XOVER": {"asset_class": "Credit", "sub_class": "iTraxxXover", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 5)},
    "ITRAXX_SENIOR_FIN": {"asset_class": "Credit", "sub_class": "iTraxxSenFin", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 5)},
    "CDS_BASKET": {"asset_class": "Credit", "sub_class": "BasketCDS", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 7)},
    "CDS_NTH_TO_DEFAULT": {"asset_class": "Credit", "sub_class": "NthToDefault", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 7)},
    "CLN": {"asset_class": "Credit", "sub_class": "CreditLinkedNote", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 10)},
    "TRS_BOND": {"asset_class": "Credit", "sub_class": "TotalReturnSwap", "sa_ccr_category": "CR_SN", "typical_tenor_years": (0.5, 5)},
    "TRS_LOAN": {"asset_class": "Credit", "sub_class": "TRSLoan", "sa_ccr_category": "CR_SN", "typical_tenor_years": (0.5, 5)},
    "CDS_OPTION": {"asset_class": "Credit", "sub_class": "CDSOption", "sa_ccr_category": "CR_SN", "typical_tenor_years": (0.25, 3)},
    "CDS_FORWARD": {"asset_class": "Credit", "sub_class": "CDSForward", "sa_ccr_category": "CR_SN", "typical_tenor_years": (0.25, 2)},
    "LCDS": {"asset_class": "Credit", "sub_class": "LoanCDS", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 7)},
    "LCDX": {"asset_class": "Credit", "sub_class": "LoanCDSIndex", "sa_ccr_category": "CR_IDX", "typical_tenor_years": (1, 5)},
    "CMCDS": {"asset_class": "Credit", "sub_class": "ConstantMaturityCDS", "sa_ccr_category": "CR_SN", "typical_tenor_years": (1, 10)},
    
    # =========================================================================
    # EQUITY DERIVATIVES (15 products)
    # =========================================================================
    "EQ_SWAP_PRICE_RETURN": {"asset_class": "Equity", "sub_class": "PriceReturnSwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.5, 5)},
    "EQ_SWAP_TOTAL_RETURN": {"asset_class": "Equity", "sub_class": "TotalReturnSwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.5, 5)},
    "EQ_OPTION_VANILLA": {"asset_class": "Equity", "sub_class": "VanillaOption", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 3)},
    "EQ_OPTION_BARRIER": {"asset_class": "Equity", "sub_class": "BarrierOption", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 2)},
    "EQ_OPTION_ASIAN": {"asset_class": "Equity", "sub_class": "AsianOption", "sa_ccr_category": "EQ", "typical_tenor_years": (0.25, 1)},
    "EQ_OPTION_LOOKBACK": {"asset_class": "Equity", "sub_class": "LookbackOption", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 1)},
    "EQ_FORWARD": {"asset_class": "Equity", "sub_class": "Forward", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 2)},
    "EQ_FUTURE": {"asset_class": "Equity", "sub_class": "Future", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 1)},
    "EQ_INDEX_OPTION": {"asset_class": "Equity", "sub_class": "IndexOption", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 2)},
    "EQ_INDEX_FUTURE": {"asset_class": "Equity", "sub_class": "IndexFuture", "sa_ccr_category": "EQ", "typical_tenor_years": (0.1, 1)},
    "VARIANCE_SWAP": {"asset_class": "Equity", "sub_class": "VarianceSwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.25, 3)},
    "VOLATILITY_SWAP": {"asset_class": "Equity", "sub_class": "VolatilitySwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.25, 2)},
    "DIVIDEND_SWAP": {"asset_class": "Equity", "sub_class": "DividendSwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.5, 5)},
    "CORRELATION_SWAP": {"asset_class": "Equity", "sub_class": "CorrelationSwap", "sa_ccr_category": "EQ", "typical_tenor_years": (0.5, 3)},
    "EQ_CFD": {"asset_class": "Equity", "sub_class": "CFD", "sa_ccr_category": "EQ", "typical_tenor_years": (0.01, 0.5)},
    
    # =========================================================================
    # COMMODITY DERIVATIVES (12 products)
    # =========================================================================
    "COMMODITY_SWAP": {"asset_class": "Commodity", "sub_class": "Swap", "sa_ccr_category": "CO", "typical_tenor_years": (0.25, 5)},
    "COMMODITY_FORWARD": {"asset_class": "Commodity", "sub_class": "Forward", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 3)},
    "COMMODITY_OPTION": {"asset_class": "Commodity", "sub_class": "Option", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 2)},
    "COMMODITY_FUTURE": {"asset_class": "Commodity", "sub_class": "Future", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 2)},
    "COMMODITY_SWAPTION": {"asset_class": "Commodity", "sub_class": "Swaption", "sa_ccr_category": "CO", "typical_tenor_years": (0.25, 2)},
    "COMMODITY_ASIAN_OPTION": {"asset_class": "Commodity", "sub_class": "AsianOption", "sa_ccr_category": "CO", "typical_tenor_years": (0.25, 1)},
    "COMMODITY_SPREAD_OPTION": {"asset_class": "Commodity", "sub_class": "SpreadOption", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 2)},
    "COMMODITY_BASIS_SWAP": {"asset_class": "Commodity", "sub_class": "BasisSwap", "sa_ccr_category": "CO", "typical_tenor_years": (0.25, 3)},
    "COMMODITY_INDEX_SWAP": {"asset_class": "Commodity", "sub_class": "IndexSwap", "sa_ccr_category": "CO", "typical_tenor_years": (0.5, 5)},
    "PRECIOUS_METAL_SWAP": {"asset_class": "Commodity", "sub_class": "PreciousMetalSwap", "sa_ccr_category": "CO", "typical_tenor_years": (0.25, 5)},
    "PRECIOUS_METAL_FORWARD": {"asset_class": "Commodity", "sub_class": "PreciousMetalForward", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 2)},
    "PRECIOUS_METAL_OPTION": {"asset_class": "Commodity", "sub_class": "PreciousMetalOption", "sa_ccr_category": "CO", "typical_tenor_years": (0.1, 2)},
}

# =============================================================================
# REFERENCE DATA
# =============================================================================

CURRENCIES = [
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK",
    "DKK", "SGD", "HKD", "KRW", "CNY", "INR", "BRL", "MXN", "ZAR", "PLN"
]

MAJOR_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF"]

CURRENCY_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD",
    "EUR/GBP", "EUR/JPY", "GBP/JPY", "EUR/CHF", "AUD/JPY", "CAD/JPY",
    "EUR/AUD", "GBP/CHF", "USD/SGD", "USD/HKD", "EUR/SEK", "USD/NOK", "USD/MXN"
]

NDF_CURRENCIES = ["CNY", "INR", "BRL", "KRW", "TWD", "IDR", "MYR", "PHP", "CLP", "COP"]

CREDIT_RATINGS = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "CCC", "NR"]

CREDIT_INDICES = ["CDX.NA.IG", "CDX.NA.HY", "iTraxx.EUR.Main", "iTraxx.EUR.Xover", "iTraxx.EUR.SenFin", "CDX.EM"]

EQUITY_INDICES = ["SPX", "NDX", "DJI", "RTY", "STOXX50E", "UKX", "NKY", "HSI", "KOSPI", "ASX200"]

COMMODITY_TYPES = [
    ("ENERGY", ["WTI", "BRENT", "NATGAS", "HEATING_OIL", "RBOB", "GASOIL"]),
    ("METALS", ["GOLD", "SILVER", "PLATINUM", "PALLADIUM", "COPPER", "ALUMINUM"]),
    ("AGRICULTURE", ["CORN", "WHEAT", "SOYBEANS", "COFFEE", "SUGAR", "COTTON"]),
]

REFERENCE_ENTITIES = [
    "APPLE_INC", "MICROSOFT_CORP", "AMAZON_COM", "ALPHABET_INC", "META_PLATFORMS",
    "JPMORGAN_CHASE", "BANK_OF_AMERICA", "WELLS_FARGO", "CITIGROUP", "GOLDMAN_SACHS",
    "GENERAL_ELECTRIC", "GENERAL_MOTORS", "FORD_MOTOR", "BOEING_CO", "CATERPILLAR",
    "EXXONMOBIL", "CHEVRON", "CONOCOPHILLIPS", "SCHLUMBERGER", "HALLIBURTON",
    "ATT_INC", "VERIZON", "COMCAST", "DISNEY", "NETFLIX",
    "PFIZER", "JOHNSON_JOHNSON", "UNITEDHEALTH", "CVS_HEALTH", "MERCK",
    "WALMART", "HOME_DEPOT", "COSTCO", "TARGET", "MCDONALDS",
    "COCA_COLA", "PEPSICO", "PROCTER_GAMBLE", "NIKE", "STARBUCKS",
]

COUNTERPARTIES = [
    ("CP001", "Goldman Sachs International", "GS"),
    ("CP002", "JPMorgan Chase Bank NA", "JPM"),
    ("CP003", "Bank of America NA", "BAC"),
    ("CP004", "Citibank NA", "C"),
    ("CP005", "Morgan Stanley Co LLC", "MS"),
    ("CP006", "Wells Fargo Bank NA", "WFC"),
    ("CP007", "Deutsche Bank AG", "DB"),
    ("CP008", "UBS AG", "UBS"),
    ("CP009", "Credit Suisse AG", "CS"),
    ("CP010", "Barclays Bank PLC", "BARC"),
    ("CP011", "HSBC Bank PLC", "HSBA"),
    ("CP012", "BNP Paribas SA", "BNP"),
    ("CP013", "Societe Generale SA", "GLE"),
    ("CP014", "Nomura Securities", "NMR"),
    ("CP015", "Mizuho Bank Ltd", "MFG"),
    ("CP016", "Sumitomo Mitsui Banking", "SMFG"),
    ("CP017", "Royal Bank of Canada", "RY"),
    ("CP018", "Toronto Dominion Bank", "TD"),
    ("CP019", "ING Bank NV", "ING"),
    ("CP020", "Standard Chartered PLC", "STAN"),
    ("CP021", "State Street Corp", "STT"),
    ("CP022", "Northern Trust Corp", "NTRS"),
    ("CP023", "Bank of New York Mellon", "BK"),
    ("CP024", "Macquarie Bank Ltd", "MQG"),
    ("CP025", "Commonwealth Bank Australia", "CBA"),
    ("CP026", "ANZ Banking Group", "ANZ"),
    ("CP027", "Westpac Banking Corp", "WBC"),
    ("CP028", "DBS Bank Ltd", "DBS"),
    ("CP029", "OCBC Bank", "OCBC"),
    ("CP030", "United Overseas Bank", "UOB"),
]

TRADING_DESKS = [
    "RATES_NY", "RATES_LDN", "RATES_TKY", "RATES_HK", "RATES_SG",
    "FX_NY", "FX_LDN", "FX_TKY", "FX_HK", "FX_SG",
    "CREDIT_NY", "CREDIT_LDN", "CREDIT_TKY",
    "EQUITY_NY", "EQUITY_LDN", "EQUITY_TKY", "EQUITY_HK",
    "COMMODITY_NY", "COMMODITY_LDN", "COMMODITY_SG",
]

LEGAL_ENTITIES = [
    ("LE001", "Acme Bank NA", "US"),
    ("LE002", "Acme Bank London Branch", "GB"),
    ("LE003", "Acme Bank Tokyo Branch", "JP"),
    ("LE004", "Acme Securities LLC", "US"),
    ("LE005", "Acme International Ltd", "GB"),
]

BOOKS = [f"BOOK_{i:04d}" for i in range(1, 201)]

CSA_AGREEMENTS = [f"CSA_{i:05d}" for i in range(1, 501)]

NETTING_SETS = [f"NS_{i:05d}" for i in range(1, 1001)]

CLEARING_HOUSES = ["LCH", "CME", "ICE", "EUREX", "JSCC", "SGX", "BILATERAL"]

# =============================================================================
# SA-CCR PARAMETERS
# =============================================================================

SA_CCR_SUPERVISORY_FACTORS = {
    "IR": {"SF": 0.005, "correlation": 0.5},
    "FX": {"SF": 0.04, "correlation": 1.0},
    "CR_SN": {"SF": 0.05, "correlation": 0.5},  # Single name credit
    "CR_IDX": {"SF": 0.03, "correlation": 0.8},  # Credit index
    "EQ": {"SF": 0.32, "correlation": 0.75},
    "CO": {"SF": 0.18, "correlation": 0.4},
}

# =============================================================================
# TRADE DATA GENERATOR
# =============================================================================

class TradeGenerator:
    """
    Generates synthetic trade data for CCR calculations.
    
    Supports 80+ trading products across 5 asset classes:
    - Interest Rate (25 products)
    - FX (18 products)
    - Credit (18 products)
    - Equity (15 products)
    - Commodity (12 products)
    """
    
    def __init__(self, seed: Optional[int] = None):
        """Initialize generator with optional random seed."""
        if seed is not None:
            random.seed(seed)
        self.trade_counter = 0
        self.base_date = datetime.now()
    
    def generate_trade_id(self) -> str:
        """Generate unique trade ID."""
        self.trade_counter += 1
        return f"TRD{self.trade_counter:010d}"
    
    def generate_external_id(self) -> str:
        """Generate external reference ID."""
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choices(chars, k=12))
    
    def random_date(self, start_days: int, end_days: int) -> str:
        """Generate random date within range from base_date."""
        days = random.randint(start_days, end_days)
        return (self.base_date + timedelta(days=days)).strftime("%Y-%m-%d")
    
    def generate_notional(self, product_type: str) -> float:
        """Generate realistic notional based on product type."""
        product_info = PRODUCTS.get(product_type, {})
        asset_class = product_info.get("asset_class", "")
        
        if asset_class == "InterestRate":
            return round(random.choice([1, 5, 10, 25, 50, 100, 250, 500]) * 1_000_000, 2)
        elif asset_class == "ForeignExchange":
            return round(random.choice([1, 5, 10, 25, 50, 100]) * 1_000_000, 2)
        elif asset_class == "Credit":
            return round(random.choice([1, 5, 10, 25, 50]) * 1_000_000, 2)
        elif asset_class == "Equity":
            return round(random.choice([1, 5, 10, 25]) * 1_000_000, 2)
        elif asset_class == "Commodity":
            return round(random.choice([1, 5, 10, 25, 50]) * 1_000_000, 2)
        else:
            return round(random.uniform(1_000_000, 100_000_000), 2)
    
    def generate_mtm(self, notional: float) -> float:
        """Generate realistic MTM value."""
        # MTM typically -5% to +5% of notional with some outliers
        pct = random.gauss(0, 0.02)  # Mean 0, std 2%
        pct = max(-0.15, min(0.15, pct))  # Cap at ±15%
        return round(notional * pct, 2)
    
    def generate_trade(self, product_type: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single trade record."""
        
        # Select product
        if product_type is None:
            product_type = random.choice(list(PRODUCTS.keys()))
        
        product_info = PRODUCTS[product_type]
        asset_class = product_info["asset_class"]
        sub_class = product_info["sub_class"]
        sa_ccr_category = product_info["sa_ccr_category"]
        tenor_range = product_info["typical_tenor_years"]
        
        # Counterparty
        cp_id, cp_name, cp_ticker = random.choice(COUNTERPARTIES)
        cp_rating = random.choice(CREDIT_RATINGS[:12])  # Mostly investment grade
        
        # Legal entity
        le_id, le_name, le_country = random.choice(LEGAL_ENTITIES)
        
        # Dates
        trade_date = self.random_date(-365, 0)  # Within last year
        effective_date = trade_date
        tenor_years = random.uniform(tenor_range[0], tenor_range[1])
        maturity_days = int(tenor_years * 365)
        maturity_date = (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=maturity_days)).strftime("%Y-%m-%d")
        
        # Notional & Currency
        notional = self.generate_notional(product_type)
        notional_ccy = random.choice(CURRENCIES[:10])  # Major currencies more common
        
        # MTM
        mtm_value = self.generate_mtm(notional)
        mtm_ccy = notional_ccy
        
        # Generate product-specific attributes
        underlying = self._generate_underlying(product_type, asset_class)
        
        # Direction
        direction = random.choice(["PAY", "RECEIVE", "BUY", "SELL"])
        if "SWAP" in product_type or "IRS" in product_type:
            direction = random.choice(["PAY", "RECEIVE"])
        elif "OPTION" in product_type or "FORWARD" in product_type:
            direction = random.choice(["BUY", "SELL"])
        
        # Clearing
        is_cleared = random.random() < 0.6  # 60% cleared
        clearing_house = random.choice(CLEARING_HOUSES[:-1]) if is_cleared else "BILATERAL"
        
        # Collateral
        is_collateralized = is_cleared or (random.random() < 0.8)  # Most bilateral also collateralized
        csa_id = random.choice(CSA_AGREEMENTS) if is_collateralized and not is_cleared else ""
        collateral_type = random.choice(["CASH", "GOVT_BOND", "CORP_BOND", "EQUITY"]) if is_collateralized else "NONE"
        collateral_ccy = notional_ccy if is_collateralized else ""
        initial_margin = round(notional * random.uniform(0.01, 0.05), 2) if is_collateralized else 0
        variation_margin = round(abs(mtm_value) * random.uniform(0.8, 1.0), 2) if is_collateralized else 0
        
        # Netting
        netting_set_id = random.choice(NETTING_SETS)
        
        # SA-CCR specific
        sa_ccr_sf = SA_CCR_SUPERVISORY_FACTORS.get(sa_ccr_category, {"SF": 0.05})["SF"]
        maturity_factor = min(1.0, (tenor_years / 1.0) ** 0.5) if tenor_years < 1 else 1.0
        
        # Risk sensitivities (simplified)
        delta = round(notional * random.uniform(-0.01, 0.01), 2)
        gamma = round(notional * random.uniform(-0.0001, 0.0001), 2) if "OPTION" in product_type else 0
        vega = round(notional * random.uniform(-0.001, 0.001), 2) if "OPTION" in product_type else 0
        
        # Exposure metrics
        exposure_at_default = round(abs(mtm_value) + notional * sa_ccr_sf * maturity_factor, 2)
        potential_future_exposure = round(notional * sa_ccr_sf * maturity_factor * 1.4, 2)
        
        # Generate trade record
        trade = {
            # Identifiers
            "trade_id": self.generate_trade_id(),
            "external_id": self.generate_external_id(),
            "source_system": random.choice(["MUREX", "CALYPSO", "SUMMIT", "KONDOR", "FINDUR"]),
            "book_id": random.choice(BOOKS),
            "trading_desk": random.choice(TRADING_DESKS),
            
            # Product
            "product_type": product_type,
            "asset_class": asset_class,
            "sub_asset_class": sub_class,
            "underlying": underlying,
            "direction": direction,
            
            # Counterparty
            "counterparty_id": cp_id,
            "counterparty_name": cp_name,
            "counterparty_ticker": cp_ticker,
            "counterparty_rating": cp_rating,
            "counterparty_sector": random.choice(["BANK", "INSURANCE", "ASSET_MGR", "HEDGE_FUND", "CORPORATE", "SOVEREIGN"]),
            "counterparty_country": random.choice(["US", "GB", "DE", "FR", "JP", "CH", "CA", "AU", "SG", "HK"]),
            
            # Legal Entity
            "legal_entity_id": le_id,
            "legal_entity_name": le_name,
            "legal_entity_country": le_country,
            
            # Dates
            "trade_date": trade_date,
            "effective_date": effective_date,
            "maturity_date": maturity_date,
            "settlement_date": effective_date,
            "next_payment_date": self.random_date(1, 90),
            
            # Economics
            "notional": notional,
            "notional_currency": notional_ccy,
            "notional_2": notional if asset_class == "ForeignExchange" else 0,
            "notional_2_currency": random.choice([c for c in CURRENCIES[:10] if c != notional_ccy]) if asset_class == "ForeignExchange" else "",
            
            # Valuation
            "mtm_value": mtm_value,
            "mtm_currency": mtm_ccy,
            "mtm_date": self.base_date.strftime("%Y-%m-%d"),
            "accrued_interest": round(notional * random.uniform(0, 0.005), 2) if asset_class == "InterestRate" else 0,
            
            # Rates (for IR products)
            "fixed_rate": round(random.uniform(0.01, 0.06), 6) if "IRS" in product_type or "SWAP" in product_type else 0,
            "spread": round(random.uniform(-0.005, 0.01), 6) if asset_class == "InterestRate" else 0,
            "floating_rate_index": random.choice(["SOFR", "SONIA", "EURIBOR", "TIBOR", "LIBOR_3M"]) if asset_class == "InterestRate" else "",
            
            # Option specifics
            "strike_price": round(random.uniform(0.8, 1.2) * 100, 4) if "OPTION" in product_type else 0,
            "option_type": random.choice(["CALL", "PUT"]) if "OPTION" in product_type else "",
            "option_style": random.choice(["EUROPEAN", "AMERICAN", "BERMUDAN"]) if "OPTION" in product_type else "",
            "barrier_level": round(random.uniform(0.7, 1.3) * 100, 4) if "BARRIER" in product_type else 0,
            "barrier_type": random.choice(["UP_IN", "UP_OUT", "DOWN_IN", "DOWN_OUT"]) if "BARRIER" in product_type else "",
            
            # Clearing & Settlement
            "is_cleared": is_cleared,
            "clearing_house": clearing_house,
            "clearing_member": random.choice(["GSCO", "JPMC", "BAML", "CITI", "MS"]) if is_cleared else "",
            "settlement_type": random.choice(["PHYSICAL", "CASH"]),
            
            # Collateral
            "is_collateralized": is_collateralized,
            "csa_id": csa_id,
            "collateral_type": collateral_type,
            "collateral_currency": collateral_ccy,
            "initial_margin": initial_margin,
            "variation_margin": variation_margin,
            "margin_frequency": random.choice(["DAILY", "WEEKLY"]) if is_collateralized else "",
            "threshold": round(random.choice([0, 1, 5, 10]) * 1_000_000, 2) if is_collateralized else 0,
            "minimum_transfer_amount": round(random.choice([0.1, 0.25, 0.5, 1]) * 1_000_000, 2) if is_collateralized else 0,
            
            # Netting
            "netting_set_id": netting_set_id,
            "netting_agreement_type": random.choice(["ISDA_2002", "ISDA_1992", "GMRA", "GMSLA"]),
            
            # SA-CCR Regulatory
            "sa_ccr_asset_class": sa_ccr_category,
            "sa_ccr_supervisory_factor": sa_ccr_sf,
            "maturity_factor": round(maturity_factor, 6),
            "delta_adjustment": round(random.uniform(0.5, 1.0), 6),
            "supervisory_duration": round(tenor_years * (1 - (1 / (1 + 0.05) ** tenor_years)) / 0.05, 6) if tenor_years > 0 else 0,
            
            # Risk Metrics
            "delta": delta,
            "gamma": gamma,
            "vega": vega,
            "theta": round(notional * random.uniform(-0.0001, 0), 2) if "OPTION" in product_type else 0,
            "rho": round(notional * random.uniform(-0.001, 0.001), 2),
            
            # Exposure
            "current_exposure": max(0, mtm_value),
            "potential_future_exposure": potential_future_exposure,
            "exposure_at_default": exposure_at_default,
            "expected_exposure": round(exposure_at_default * 0.5, 2),
            "effective_expected_exposure": round(exposure_at_default * 0.6, 2),
            
            # Status
            "trade_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "PENDING", "AMENDED"]),
            "lifecycle_status": random.choice(["LIVE", "LIVE", "LIVE", "MATURED", "NOVATED"]),
            "confirmation_status": random.choice(["CONFIRMED", "CONFIRMED", "CONFIRMED", "UNCONFIRMED", "AFFIRMED"]),
            
            # Timestamps
            "created_timestamp": (self.base_date - timedelta(days=random.randint(0, 365))).isoformat(),
            "updated_timestamp": self.base_date.isoformat(),
            "version": random.randint(1, 5),
        }
        
        return trade
    
    def _generate_underlying(self, product_type: str, asset_class: str) -> str:
        """Generate underlying reference based on asset class."""
        if asset_class == "InterestRate":
            ccy = random.choice(CURRENCIES[:10])
            tenor = random.choice(["1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"])
            return f"{ccy}_{tenor}"
        elif asset_class == "ForeignExchange":
            return random.choice(CURRENCY_PAIRS)
        elif asset_class == "Credit":
            if "INDEX" in product_type or "CDX" in product_type or "ITRAXX" in product_type:
                return random.choice(CREDIT_INDICES)
            else:
                return random.choice(REFERENCE_ENTITIES)
        elif asset_class == "Equity":
            if "INDEX" in product_type:
                return random.choice(EQUITY_INDICES)
            else:
                return random.choice(REFERENCE_ENTITIES)
        elif asset_class == "Commodity":
            commodity_group, commodities = random.choice(COMMODITY_TYPES)
            return random.choice(commodities)
        else:
            return "UNKNOWN"
    
    def generate_trades(self, count: int = 200_000, 
                       product_mix: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
        """
        Generate multiple trade records.
        
        Args:
            count: Number of trades to generate (default: 200,000)
            product_mix: Optional dict of product_type -> weight for custom mix
            
        Returns:
            List of trade dictionaries
        """
        trades = []
        
        # Default product mix (weighted by typical portfolio composition)
        if product_mix is None:
            # IR heavy, then FX, Credit, Equity, Commodity
            product_weights = {}
            for product, info in PRODUCTS.items():
                ac = info["asset_class"]
                if ac == "InterestRate":
                    product_weights[product] = 3.0
                elif ac == "ForeignExchange":
                    product_weights[product] = 2.5
                elif ac == "Credit":
                    product_weights[product] = 2.0
                elif ac == "Equity":
                    product_weights[product] = 1.5
                elif ac == "Commodity":
                    product_weights[product] = 1.0
        else:
            product_weights = product_mix
        
        products = list(product_weights.keys())
        weights = list(product_weights.values())
        
        print(f"Generating {count:,} trades...")
        
        for i in range(count):
            product = random.choices(products, weights=weights, k=1)[0]
            trade = self.generate_trade(product)
            trades.append(trade)
            
            # Progress indicator
            if (i + 1) % 50000 == 0:
                print(f"  Generated {i + 1:,} trades...")
        
        print(f"Completed generating {count:,} trades.")
        return trades


def generate_trades(count: int = 200_000, 
                   seed: Optional[int] = None,
                   output_file: str = "trades.csv",
                   delimiter: str = "|",
                   output_format: str = "csv") -> str:
    """
    Convenience function to generate trades and save to file.
    
    Args:
        count: Number of trades to generate
        seed: Random seed for reproducibility
        output_file: Output file path
        delimiter: CSV delimiter (default: pipe |)
        output_format: Output format: 'csv' or 'json'
        
    Returns:
        Path to the generated file
    """
    generator = TradeGenerator(seed=seed)
    trades = generator.generate_trades(count)
    
    if output_format.lower() == "json":
        output_file = output_file.replace(".csv", ".json") if output_file.endswith(".csv") else output_file
        print(f"Writing {len(trades):,} trades to {output_file} (JSON format)...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(trades, f, indent=2, default=str)
    else:
        print(f"Writing {len(trades):,} trades to {output_file} (delimiter: '{delimiter}')...")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if trades:
                writer = csv.DictWriter(f, fieldnames=trades[0].keys(), delimiter=delimiter)
                writer.writeheader()
                writer.writerows(trades)
    
    print(f"Successfully wrote {len(trades):,} trades to {output_file}")
    
    # Print summary statistics
    print("\n=== Trade Generation Summary ===")
    asset_class_counts = {}
    product_counts = {}
    for trade in trades:
        ac = trade["asset_class"]
        pt = trade["product_type"]
        asset_class_counts[ac] = asset_class_counts.get(ac, 0) + 1
        product_counts[pt] = product_counts.get(pt, 0) + 1
    
    print("\nBy Asset Class:")
    for ac, cnt in sorted(asset_class_counts.items(), key=lambda x: -x[1]):
        print(f"  {ac}: {cnt:,} ({cnt/len(trades)*100:.1f}%)")
    
    print(f"\nTotal Products Used: {len(product_counts)}")
    print(f"Available Products: {len(PRODUCTS)}")
    
    return output_file


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic trade data for CCR calculations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python trade_generator.py
  python trade_generator.py --count 500000 --output large_trades.csv
  python trade_generator.py --count 10000 --format json --output trades.json
  python trade_generator.py --count 100000 --seed 42 --delimiter ","
        """
    )
    
    parser.add_argument("--count", "-n", type=int, default=200_000,
                       help="Number of trades to generate (default: 200000)")
    parser.add_argument("--output", "-o", type=str, default="trades.csv",
                       help="Output file path (default: trades.csv)")
    parser.add_argument("--delimiter", "-d", type=str, default="|",
                       help="CSV delimiter (default: |)")
    parser.add_argument("--seed", "-s", type=int, default=None,
                       help="Random seed for reproducibility")
    parser.add_argument("--format", "-f", type=str, choices=["csv", "json"], default="csv",
                       help="Output format (default: csv)")
    parser.add_argument("--list-products", action="store_true",
                       help="List all available products and exit")
    
    args = parser.parse_args()
    
    if args.list_products:
        print(f"\n{'='*80}")
        print(f"AVAILABLE TRADING PRODUCTS ({len(PRODUCTS)} total)")
        print(f"{'='*80}\n")
        
        by_asset_class = {}
        for product, info in PRODUCTS.items():
            ac = info["asset_class"]
            if ac not in by_asset_class:
                by_asset_class[ac] = []
            by_asset_class[ac].append((product, info))
        
        for ac in ["InterestRate", "ForeignExchange", "Credit", "Equity", "Commodity"]:
            if ac in by_asset_class:
                products = by_asset_class[ac]
                print(f"\n{ac} ({len(products)} products)")
                print("-" * 60)
                for product, info in products:
                    print(f"  {product:<30} {info['sub_class']:<20} SA-CCR: {info['sa_ccr_category']}")
        
        return
    
    generate_trades(
        count=args.count,
        seed=args.seed,
        output_file=args.output,
        delimiter=args.delimiter,
        output_format=args.format
    )


if __name__ == "__main__":
    main()
