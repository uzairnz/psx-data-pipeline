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
├── data/                       # Folder for storing OHLC data per ticker
│   ├── HBL.csv
│   ├── ENGRO.csv
│   └── ...
├── metadata/
│   ├── all_tickers.csv        # Latest list of tickers from PSX
│   └── ticker_changes.log     # History of additions/removals/renames
├── logs/
│   └── update_YYYY-MM-DD.log  # Daily update logs
├── scripts/
│   ├── scrape_tickers.py      # Ticker synchronization logic
│   ├── download_data.py       # Download OHLC data per ticker
│   ├── update_data.py         # Update existing data daily
│   └── utils.py               # Shared utility functions
├── config.py                  # Configurable parameters (dates, paths, etc.)
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
- [x] Compare new list with old one from `metadata/all_tickers.csv`
- [x] Log changes: additions, deletions, and renames
- [x] Update ticker CSV list accordingly

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
- [ ] Write high-level README
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
- Libraries: `pandas`, `requests`, `beautifulsoup4`, `schedule`, `argparse`
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
python psx_data_automation/main.py --version
```

### Development
For development, you can now proceed to implement the modules in the `scripts` directory:
- `scrape_tickers.py` - Fetch and manage ticker lists
- `download_data.py` - Download historical data
- `update_data.py` - Daily updates for existing data

---

## 🌟 Future Enhancements
- Add price-adjusted columns (dividends, splits)
- Add data visualization dashboard (Streamlit)
- Archive removed/delisted tickers
- Export to Parquet or Feather for faster I/O
- Add backfill checks for recent missing data

---

## 📍 Author
Uzair / [github.com/uzairnz](https://github.com/uzairnz)

---

Ready to build? Let's start with Phase 1 ✅

