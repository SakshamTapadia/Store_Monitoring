# Store Monitoring API

FastAPI application that generates uptime/downtime reports for stores based on status data, business hours, and timezones.

## Setup

1. Install dependencies:
```bash
pip install fastapi uvicorn pandas sqlite3
```

2. Ensure CSV files are present:
   - `store_status.csv`
   - `menu_hours.csv` 
   - `timezones.csv`

## Run

```bash
python main_fixed.py
```

Server runs on `http://localhost:8001`

## API Endpoints

- `GET /` - API status
- `GET /trigger_report_get` - Generate report (returns report_id)
- `POST /trigger_report` - Generate report via POST
- `GET /get_report?report_id=<id>` - Get report status or download CSV

## Usage

1. Visit `http://localhost:8001/trigger_report_get`
2. Copy the `report_id` from response
3. Visit `http://localhost:8001/get_report?report_id=<your_id>`
4. Download CSV when status is "Complete"

## Output

CSV with columns: store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week