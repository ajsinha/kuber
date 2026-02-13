"""
Kuber Test Data Generators

This module provides test data generators for various financial instruments
and entities commonly used in trading and risk management systems.

Generators:
- trade_generator: Generate synthetic trade data for CCR calculations
- counterparty_generator: Generate synthetic counterparty data
- market_data_generator: Generate synthetic market data

Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com
Patent Pending
"""

from .trade_generator import TradeGenerator, generate_trades

__all__ = ['TradeGenerator', 'generate_trades']
__version__ = '2.4.0'
