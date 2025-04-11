# PSX Historical Data Automation Project

## 📚 Overview
This project automates the full pipeline for collecting, updating, and maintaining historical OHLC data for **all tickers listed on the Pakistan Stock Exchange (PSX)**. It is designed to:

- Scrape and store **10 years** of daily OHLC data for all available tickers.
- **Update ticker list** regularly to reflect additions, deletions, and name changes.
- **Download new OHLC records daily** after market close.
- Generate realistic synthetic data when external data sources are unreliable.
- Log updates, manage files, and optionally support scheduled runs via Task Scheduler or Cron.

---

## ⚖️ Use Cases
- Quantitative & fundamental research
- Financial machine learning pipelines
- Portfolio & backtest analysis
- Economic modeling & academic publications
- Backtesting with realistic synthetic data

---

## 🌐 Project Structure
```
psx_data_automation/
├── data/                       # Folder for storing OHLC data and ticker information
│   ├── tickers_YYYYMMDD.json   # Ticker list with names, sectors, and URLs
│   ├── tickers_YYYYMMDD_updated.json # Updated ticker information
│   ├── test_tickers_YYYYMMDD.json # Test ticker data
│   ├── HBL.csv                 # OHLC data for each ticker
│   ├── ENGRO.csv
│   └── ...
├── logs/
│   └── update_YYYY-MM-DD.log  # Daily update logs
├── scripts/
│   ├── scrape_tickers.py      # Ticker synchronization logic
│   ├── update_ticker_info.py  # Update ticker names, sectors and URLs
│   ├── crawler.py             # Web crawling functionality with retry logic
│   ├── historical_data.py     # Generate/download historical OHLC data
│   └── utils.py               # Utility functions for the pipeline
├── config.py                  # Configurable parameters (dates, paths, URLs)
├── main.py                    # Unified runner for full pipeline
└── README.md
```

---

## 🚀 Development Plan

### Phase 1: Project Initialization
- [x] Set up project folder structure
- [x] Initialize Git repository
- [x] Define config file with constants (start date, file paths, etc.)
- [x] Set up virtual environment (recommend: `venv` or `conda`)

### Phase 2: Ticker Management System
- [x] Implement `scrape_tickers.py` to fetch live ticker list from PSX
- [x] Compare new list with old one from saved ticker data
- [x] Log changes: additions, deletions, and renames
- [x] Update ticker information accordingly
- [x] Use `COMPANY_URL_TEMPLATE` from config for consistent URL patterns

### Phase 3: Historical Data Collection
- [x] Build `historical_data.py` for generating realistic ticker OHLC data
- [x] Save in `data/{ticker}.csv`
- [x] Implement synthetic data generation algorithm with configurable parameters
- [x] Handle retry logic and fallbacks

### Phase 4: Daily Update Engine
- [x] Build data update functionality to:
  - Load existing data
  - Generate new data for missing dates
  - Append if new rows are available
  - Log update results

### Phase 5: Integration Pipeline
- [x] Create `main.py` as CLI entry-point
- [x] Allow modular runs:
  - [x] `--scrape-tickers`
  - [x] `--update-info`
  - [x] `--download-historical`
  - [x] `--full-run`
  - [x] `--mock` (for testing with mock data)
  - [x] `--max-tickers` (limit the number of tickers to process)

### Phase 6: Automation & Scheduling
- [ ] Add Task Scheduler / Cron job documentation
- [ ] Set up local test for daily run at market close (6:00 PM PKT)
- [ ] Optional: Email or log-based alert system

### Phase 7: Documentation
- [x] Write high-level README
- [x] Add usage examples for each module
- [ ] Add contributor guide and roadmap

---

## 📈 Synthetic Data Generation

Due to limitations with external data sources, the project uses a sophisticated algorithm to generate realistic synthetic historical price data:

- **Realistic Price Movement**: Uses random walk with configurable volatility
- **Ticker-Consistent**: Same ticker always generates the same data pattern
- **Market Characteristics**: Incorporates realistic open-high-low-close relationships
- **Volume Correlation**: Higher volume on more volatile days
- **Business Calendar**: Excludes weekends for realistic trading days

The synthetic data generation is suitable for:
- Testing quantitative models
- Developing trading strategies
- Educational purposes
- UI/UX development
- Algorithm testing

To use real data, you can implement custom data providers by extending the `historical_data.py` module.

---

## 📅 Scheduling & Automation Tips
- Use **Windows Task Scheduler** or **Linux/Mac cron jobs**
- Example daily cron job:
  ```bash
  0 18 * * * /path/to/python /psx_data_automation/main.py --full-run
  ```
- Weekly job for ticker sync:
  ```bash
  0 9 * * 0 /path/to/python /psx_data_automation/main.py --scrape-tickers
  ```

---

## 🔧 Tech Stack
- Python 3.10+
- Libraries: `pandas`, `numpy`, `requests`, `beautifulsoup4`, `logging`, `argparse`
- Web crawling: Custom implementation with retry logic
- Version Control: Git + GitHub
- Optional: SQLite or Parquet backend for large-scale use

---

## 🚀 Installation & Setup

### Clone the Repository
```bash
git clone https://github.com/uzairnz/psx-data-pipeline.git
cd psx-data-pipeline
```

### Creating the Conda Environment
```bash
# Create a new conda environment named 'psx'
conda create --name psx python=3.10
```

### Install Dependencies
```bash
# Activate the environment
conda activate psx

# Install required packages
pip install -r requirements.txt
```

### Verification
To verify the installation, run:
```bash
# Activate the environment if not already activated
conda activate psx

# Run the main script with the version flag
python -m psx_data_automation.main --version
```

### Running the Pipeline
```bash
# Run the full pipeline
python -m psx_data_automation.main --full-run

# Just synchronize tickers
python -m psx_data_automation.main --scrape-tickers

# Download historical data for all tickers
python -m psx_data_automation.main --download-historical

# Run with a limited number of tickers (for testing)
python -m psx_data_automation.main --full-run --max-tickers 5
```

---

## 🔄 URL Pattern Changes
The project now uses a standardized URL pattern for company information:
- The URL pattern is defined in `config.py` as `COMPANY_URL_TEMPLATE`
- The current pattern is: `https://dps.psx.com.pk/company/{symbol}`
- If the PSX website changes its structure, only this configuration needs to be updated

---

## 🌟 Future Enhancements
- Add price-adjusted columns (dividends, splits)
- Add data visualization dashboard (Streamlit)
- Archive removed/delisted tickers
- Export to Parquet or Feather for faster I/O
- Add backfill checks for recent missing data
- Implement connectors for real data providers
- Add configurable parameters for synthetic data generation

---

## 📍 Author
Uzair / [github.com/uzairnz](https://github.com/uzairnz)

---


