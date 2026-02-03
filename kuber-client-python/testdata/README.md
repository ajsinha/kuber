# Kuber Test Data Generators

**Version 1.9.0**

This module provides test data generators for financial instruments commonly used in trading, risk management, and regulatory reporting systems.

## Installation

No additional dependencies required beyond Python 3.8+ standard library.

## Trade Data Generator

The `trade_generator.py` module generates synthetic trade data suitable for:
- **CCR (Counterparty Credit Risk)** calculations
- **SA-CCR (Standardized Approach for Counterparty Credit Risk)** compliance
- **CVA/DVA** calculations
- **Exposure reporting**
- **Stress testing**

### Features

- **88 Trading Products** across 5 asset classes
- **CCR-Relevant Attributes** including SA-CCR parameters
- **Realistic Data Distribution** based on typical portfolio composition
- **Configurable Output** (CSV/JSON, custom delimiters)
- **Reproducible Generation** with random seed support

### Quick Start

```bash
# Generate 200,000 trades (default)
python -m testdata.trade_generator

# Generate custom count
python -m testdata.trade_generator --count 500000

# Generate with specific seed for reproducibility
python -m testdata.trade_generator --count 100000 --seed 42

# Generate JSON format
python -m testdata.trade_generator --format json --output trades.json

# Use comma delimiter
python -m testdata.trade_generator --delimiter ","
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--count, -n` | 200000 | Number of trades to generate |
| `--output, -o` | trades.csv | Output file path |
| `--delimiter, -d` | \| (pipe) | CSV delimiter |
| `--seed, -s` | None | Random seed for reproducibility |
| `--format, -f` | csv | Output format: csv or json |
| `--list-products` | - | List all available products |

### Trading Products (88 Total)

#### Interest Rate (25 products)
| Product | Description |
|---------|-------------|
| IRS_FIXED_FLOAT | Fixed-for-floating interest rate swap |
| IRS_FLOAT_FLOAT | Basis swap (float-for-float) |
| IRS_OIS | Overnight index swap |
| IRS_XCCY | Cross-currency interest rate swap |
| IRS_INFLATION | Inflation-linked swap |
| IRS_ZCS | Zero coupon swap |
| IRS_AMORTIZING | Amortizing swap |
| FRA | Forward rate agreement |
| CAP | Interest rate cap |
| FLOOR | Interest rate floor |
| COLLAR | Interest rate collar |
| SWAPTION_PAYER | Payer swaption |
| SWAPTION_RECEIVER | Receiver swaption |
| SWAPTION_STRADDLE | Swaption straddle |
| BOND_OPTION | Bond option |
| BOND_FUTURE | Bond future |
| IR_FUTURE | Interest rate future |
| IR_FUTURE_OPTION | IR future option |
| REPO | Repurchase agreement |
| REVERSE_REPO | Reverse repo |
| BOND_FORWARD | Bond forward |
| CMS_SWAP | Constant maturity swap |
| CMS_CAP | CMS cap |
| CMS_FLOOR | CMS floor |
| CALLABLE_SWAP | Callable swap |

#### Foreign Exchange (18 products)
| Product | Description |
|---------|-------------|
| FX_SPOT | FX spot transaction |
| FX_FORWARD | FX forward |
| FX_NDF | Non-deliverable forward |
| FX_SWAP | FX swap |
| FX_OPTION_VANILLA | Vanilla FX option |
| FX_OPTION_BARRIER | Barrier option |
| FX_OPTION_DIGITAL | Digital/binary option |
| FX_OPTION_ASIAN | Asian option |
| FX_OPTION_LOOKBACK | Lookback option |
| FX_OPTION_COMPOUND | Compound option |
| FX_OPTION_CHOOSER | Chooser option |
| FX_STRADDLE | FX straddle |
| FX_STRANGLE | FX strangle |
| FX_RISK_REVERSAL | Risk reversal |
| FX_BUTTERFLY | Butterfly spread |
| FX_SEAGULL | Seagull strategy |
| FX_TARGET_REDEMPTION | Target redemption forward |
| FX_ACCUMULATOR | FX accumulator |

#### Credit (18 products)
| Product | Description |
|---------|-------------|
| CDS_SINGLE_NAME | Single-name CDS |
| CDS_INDEX | Index CDS |
| CDS_INDEX_TRANCHE | Index tranche |
| CDX_IG | CDX Investment Grade |
| CDX_HY | CDX High Yield |
| ITRAXX_MAIN | iTraxx Main |
| ITRAXX_XOVER | iTraxx Crossover |
| ITRAXX_SENIOR_FIN | iTraxx Senior Financials |
| CDS_BASKET | Basket CDS |
| CDS_NTH_TO_DEFAULT | Nth-to-default |
| CLN | Credit-linked note |
| TRS_BOND | Total return swap (bond) |
| TRS_LOAN | Total return swap (loan) |
| CDS_OPTION | CDS option |
| CDS_FORWARD | CDS forward |
| LCDS | Loan CDS |
| LCDX | Loan CDS index |
| CMCDS | Constant maturity CDS |

#### Equity (15 products)
| Product | Description |
|---------|-------------|
| EQ_SWAP_PRICE_RETURN | Price return swap |
| EQ_SWAP_TOTAL_RETURN | Total return swap |
| EQ_OPTION_VANILLA | Vanilla equity option |
| EQ_OPTION_BARRIER | Barrier option |
| EQ_OPTION_ASIAN | Asian option |
| EQ_OPTION_LOOKBACK | Lookback option |
| EQ_FORWARD | Equity forward |
| EQ_FUTURE | Equity future |
| EQ_INDEX_OPTION | Index option |
| EQ_INDEX_FUTURE | Index future |
| VARIANCE_SWAP | Variance swap |
| VOLATILITY_SWAP | Volatility swap |
| DIVIDEND_SWAP | Dividend swap |
| CORRELATION_SWAP | Correlation swap |
| EQ_CFD | Contract for difference |

#### Commodity (12 products)
| Product | Description |
|---------|-------------|
| COMMODITY_SWAP | Commodity swap |
| COMMODITY_FORWARD | Commodity forward |
| COMMODITY_OPTION | Commodity option |
| COMMODITY_FUTURE | Commodity future |
| COMMODITY_SWAPTION | Commodity swaption |
| COMMODITY_ASIAN_OPTION | Asian option |
| COMMODITY_SPREAD_OPTION | Spread option |
| COMMODITY_BASIS_SWAP | Basis swap |
| COMMODITY_INDEX_SWAP | Index swap |
| PRECIOUS_METAL_SWAP | Precious metal swap |
| PRECIOUS_METAL_FORWARD | Precious metal forward |
| PRECIOUS_METAL_OPTION | Precious metal option |

### Generated Trade Attributes

The generator produces trades with **70+ attributes** covering:

#### Identifiers
- `trade_id` - Unique trade identifier
- `external_id` - External reference ID
- `source_system` - Source trading system
- `book_id` - Trading book
- `trading_desk` - Desk identifier

#### Product Details
- `product_type` - Product identifier
- `asset_class` - Asset class (IR, FX, Credit, Equity, Commodity)
- `sub_asset_class` - Sub-classification
- `underlying` - Underlying reference
- `direction` - PAY/RECEIVE or BUY/SELL

#### Counterparty
- `counterparty_id` - Counterparty identifier
- `counterparty_name` - Legal name
- `counterparty_ticker` - Stock ticker
- `counterparty_rating` - Credit rating
- `counterparty_sector` - Industry sector
- `counterparty_country` - Jurisdiction

#### Dates
- `trade_date` - Trade execution date
- `effective_date` - Effective start date
- `maturity_date` - Final maturity
- `settlement_date` - Settlement date
- `next_payment_date` - Next cashflow date

#### Economics
- `notional` - Notional amount
- `notional_currency` - Notional currency
- `fixed_rate` - Fixed rate (for IR products)
- `spread` - Spread over benchmark
- `floating_rate_index` - Floating rate index

#### Valuation
- `mtm_value` - Mark-to-market value
- `mtm_currency` - MTM currency
- `accrued_interest` - Accrued interest

#### Collateral
- `is_collateralized` - Collateral flag
- `csa_id` - CSA agreement ID
- `collateral_type` - Collateral type
- `initial_margin` - Initial margin
- `variation_margin` - Variation margin
- `threshold` - Credit threshold
- `minimum_transfer_amount` - MTA

#### SA-CCR Parameters
- `sa_ccr_asset_class` - SA-CCR asset class
- `sa_ccr_supervisory_factor` - Supervisory factor
- `maturity_factor` - Maturity factor
- `delta_adjustment` - Delta adjustment
- `supervisory_duration` - Supervisory duration

#### Risk Metrics
- `delta` - Delta sensitivity
- `gamma` - Gamma sensitivity
- `vega` - Vega sensitivity
- `theta` - Theta sensitivity
- `rho` - Rho sensitivity

#### Exposure
- `current_exposure` - Current exposure
- `potential_future_exposure` - PFE
- `exposure_at_default` - EAD
- `expected_exposure` - EE
- `effective_expected_exposure` - EEE

### Python API Usage

```python
from testdata import TradeGenerator, generate_trades

# Quick generation
generate_trades(count=100000, output_file="trades.csv")

# Advanced usage with generator class
generator = TradeGenerator(seed=42)

# Generate single trade
trade = generator.generate_trade(product_type="IRS_FIXED_FLOAT")

# Generate batch with custom product mix
custom_mix = {
    "IRS_FIXED_FLOAT": 5.0,
    "FX_FORWARD": 3.0,
    "CDS_SINGLE_NAME": 2.0,
}
trades = generator.generate_trades(count=50000, product_mix=custom_mix)
```

### Loading into Kuber

After generating trade data, load it into Kuber cache:

```bash
# Copy to Kuber inbox for autoload
cp trades.csv /path/to/kuber/inbox/trades.csv

# Or use Python client
from kuber import KuberRestClient

client = KuberRestClient("http://localhost:8080", api_key="your-key")

# Load trades
import csv
with open("trades.csv", "r") as f:
    reader = csv.DictReader(f, delimiter="|")
    for trade in reader:
        client.set("trades", trade["trade_id"], json.dumps(trade))
```

### Sample Output

```
trade_id|external_id|source_system|book_id|product_type|asset_class|...
TRD0000000001|A7B3C9D2E1F4|MUREX|BOOK_0042|IRS_FIXED_FLOAT|InterestRate|...
TRD0000000002|X1Y2Z3W4V5U6|CALYPSO|BOOK_0108|FX_FORWARD|ForeignExchange|...
TRD0000000003|M9N8O7P6Q5R4|SUMMIT|BOOK_0023|CDS_INDEX|Credit|...
```

---

**Copyright © 2025-2030, All Rights Reserved**  
Ashutosh Sinha | [ajsinha@gmail.com](mailto:ajsinha@gmail.com)  
*Patent Pending*
