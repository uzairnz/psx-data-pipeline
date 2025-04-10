# PSX Historical Data Automation Project

## 📚 Overview
This project automates the full pipeline for collecting, updating, and maintaining historical OHLC data for **all tickers listed on the Pakistan Stock Exchange (PSX)**. It is designed to:

- Scrape and store **10 years** of daily OHLC data for all available tickers.
- **Update ticker list** regularly to reflect additions, deletions, and name changes.
- **Download new OHLC records daily** after market close.
- Log updates, manage files, and optionally support scheduled runs via Task Scheduler or Cron.

---

## ⚖️ Use Cases
- Quantitative & fundamental research
- Financial machine learning pipelines
- Portfolio & backtest analysis
- Economic modeling & academic publications

---

## 🌐 Project Structure
```
psx_data_automation/
├── data/                       # Folder for storing OHLC data and ticker information
│   ├── tickers_YYYYMMDD.json   # Ticker list with names, sectors, and URLs
│   ├── tickers_YYYYMMDD_updated.json # Updated ticker information
│   ├── test_tickers_YYYYMMDD.json # Test ticker data
│   ├── HBL.csv                 # OHLC data for each ticker (future)
│   ├── ENGRO.csv
│   └── ...
├── logs/
│   └── update_YYYY-MM-DD.log  # Daily update logs
├── scripts/
│   ├── scrape_tickers.py      # Ticker synchronization logic
│   ├── update_ticker_info.py  # Update ticker names, sectors and URLs
│   ├── download_data.py       # Download OHLC data per ticker (future)
│   ├── update_data.py         # Update existing data daily (future)
│   └── test_ticker_update.py  # Test script for ticker updates
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
- [ ] Build `download_data.py` to fetch 10 years of daily OHLC data per ticker
- [ ] Save in `data/{ticker}.csv`
- [ ] Handle missing data and retry logic

### Phase 4: Daily Update Engine
- [ ] Build `update_data.py` to:
  - Load last date from each CSV
  - Fetch daily data from last date + 1 to today
  - Append if new rows are available
  - Log update results

### Phase 5: Integration Pipeline
- [x] Create `main.py` as CLI entry-point
- [x] Allow modular runs:
  - [x] `--sync-tickers`
  - [ ] `--download-historical`
  - [ ] `--daily-update`
  - [x] `--full-run`

### Phase 6: Automation & Scheduling
- [ ] Add Task Scheduler / Cron job documentation
- [ ] Set up local test for daily run at market close (6:00 PM PKT)
- [ ] Optional: Email or log-based alert system

### Phase 7: Documentation
- [x] Write high-level README
- [ ] Add usage examples for each module
- [ ] Add contributor guide and roadmap

---

## 📅 Scheduling & Automation Tips
- Use **Windows Task Scheduler** or **Linux/Mac cron jobs**
- Example daily cron job:
  ```bash
  0 18 * * * /path/to/python /psx_data_automation/main.py --full-run
  ```
- Weekly job for ticker sync:
  ```bash
  0 9 * * 0 /path/to/python /psx_data_automation/main.py --sync-tickers
  ```

---

## 🔧 Tech Stack
- Python 3.10+
- Libraries: `pandas`, `requests`, `beautifulsoup4`, `logging`, `argparse`
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
python -m psx_data_automation.main --sync-tickers
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
- Handle tickers with non-standard URL patterns or errors

---

## 📍 Author
Uzair / [github.com/uzairnz](https://github.com/uzairnz)

---


