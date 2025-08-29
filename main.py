from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse
import uuid
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta, timezone
import zoneinfo


DB_FILE = "store_monitoring.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS store_status(
        store_id TEXT, timestamp_utc TEXT, status TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS business_hours(
        store_id TEXT, day_of_week INTEGER, start_time_local TEXT, end_time_local TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS store_timezone(
        store_id TEXT, timezone_str TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS report_status(
        report_id TEXT PRIMARY KEY, status TEXT, csv_path TEXT
    )""")
    conn.commit()
    conn.close()

def ingest_csvs():
    conn = sqlite3.connect(DB_FILE)
    
    conn.execute("DELETE FROM store_status")
    conn.execute("DELETE FROM business_hours") 
    conn.execute("DELETE FROM store_timezone")
    
    try:
        status_df = pd.read_csv("store_status.csv")
        status_df = status_df[['store_id', 'timestamp_utc', 'status']]
        status_df.to_sql("store_status", conn, if_exists='append', index=False)
        
        hours_df = pd.read_csv("menu_hours.csv")
        hours_df = hours_df.rename(columns={'dayOfWeek': 'day_of_week'})
        hours_df.to_sql("business_hours", conn, if_exists='append', index=False)
        
        tz_df = pd.read_csv("timezones.csv")
        tz_df.to_sql("store_timezone", conn, if_exists='append', index=False)
        
        print("Data ingested successfully")
    except Exception as e:
        print(f"Error ingesting data: {e}")
    finally:
        conn.close()

def compute_store_metrics(store_id, current_utc, status_df, business_df, timezone_df):
    """Compute uptime/downtime metrics for a store"""
    
    store_status = status_df[status_df['store_id'] == store_id].copy()
    store_business = business_df[business_df['store_id'] == store_id]
    store_tz = timezone_df[timezone_df['store_id'] == store_id]
    
    timezone_str = store_tz['timezone_str'].iloc[0] if not store_tz.empty else "America/Chicago"
    try:
        tz = zoneinfo.ZoneInfo(timezone_str)
    except:
        tz = zoneinfo.ZoneInfo("America/Chicago")
    
    business_hours = {}
    if store_business.empty:
        for day in range(7):
            business_hours[day] = ("00:00:00", "23:59:59")
    else:
        for _, row in store_business.iterrows():
            business_hours[row['day_of_week']] = (row['start_time_local'], row['end_time_local'])
    
    store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'].str.replace(' UTC', '', regex=False)).dt.tz_localize('UTC')
    store_status = store_status.sort_values('timestamp_utc')
    
    one_hour_ago = current_utc - timedelta(hours=1)
    one_day_ago = current_utc - timedelta(days=1)
    one_week_ago = current_utc - timedelta(weeks=1)
    
    metrics = []
    
    for period_start, period_name in [(one_hour_ago, "hour"), (one_day_ago, "day"), (one_week_ago, "week")]:
        period_obs = store_status[
            (store_status['timestamp_utc'] >= period_start) & 
            (store_status['timestamp_utc'] <= current_utc)
        ]
        
        if period_obs.empty:
            metrics.extend([0.0, 0.0])
            continue
        total_business_minutes = 0
        uptime_minutes = 0
        
        # Simplified calculation
        if not period_obs.empty:
            last_status = period_obs.iloc[-1]['status']
            if period_name == "hour":
                total_business_minutes = 60
            elif period_name == "day":
                total_business_minutes = 12 * 60
            else:
                total_business_minutes = 7 * 12 * 60
            
            if last_status == "active":
                uptime_minutes = total_business_minutes
            else:
                uptime_minutes = 0
        
        if period_name == "hour":
            uptime = uptime_minutes
            downtime = total_business_minutes - uptime_minutes
        else: 
            uptime = uptime_minutes / 60.0
            downtime = (total_business_minutes - uptime_minutes) / 60.0
        
        metrics.extend([max(0, uptime), max(0, downtime)])
    
    return metrics

def generate_report(report_id):
    print(f"Starting report generation for {report_id}")
    try:
        conn = sqlite3.connect(DB_FILE)
        
        status_df = pd.read_sql_query("SELECT * FROM store_status", conn)
        business_df = pd.read_sql_query("SELECT * FROM business_hours", conn)
        timezone_df = pd.read_sql_query("SELECT * FROM store_timezone", conn)
        
        current_utc = pd.to_datetime(status_df['timestamp_utc'].str.replace(' UTC', '', regex=False)).dt.tz_localize('UTC').max()
        
        report_rows = []
        for store_id in status_df['store_id'].unique():
            metrics = compute_store_metrics(store_id, current_utc, status_df, business_df, timezone_df)
            report_rows.append([store_id] + metrics)
        
        csv_path = f"report_{report_id}.csv"
        df = pd.DataFrame(report_rows, columns=[
            "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week",
            "downtime_last_hour", "downtime_last_day", "downtime_last_week"
        ])
        df.to_csv(csv_path, index=False)
        print(f"Report saved to {csv_path}")
        
        conn.execute("UPDATE report_status SET status=?, csv_path=? WHERE report_id=?",
                     ("Complete", csv_path, report_id))
        conn.commit()
        print(f"Report {report_id} completed")
        
    except Exception as e:
        print(f"Error generating report: {e}")
        import traceback
        traceback.print_exc()
        conn.execute("UPDATE report_status SET status=? WHERE report_id=?", ("Failed", report_id))
        conn.commit()
    finally:
        conn.close()

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    ingest_csvs()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Store Monitoring API is running", "endpoints": ["/trigger_report", "/get_report"]}

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT INTO report_status(report_id, status, csv_path) VALUES (?, ?, ?)",
                 (report_id, "Running", ""))
    conn.commit()
    conn.close()
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/trigger_report_get")
async def trigger_report_get(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT INTO report_status(report_id, status, csv_path) VALUES (?, ?, ?)",
                 (report_id, "Running", ""))
    conn.commit()
    conn.close()
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
async def get_report(report_id: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT status, csv_path FROM report_status WHERE report_id=?", (report_id,))
    row = c.fetchone()
    conn.close()
    
    if not row:
        return {"error": "Report not found"}
    
    status, csv_path = row
    if status == "Running":
        return {"status": "Running"}
    elif status == "Complete" and os.path.exists(csv_path):
        return FileResponse(csv_path, media_type="text/csv", filename=f"report_{report_id}.csv")
    else:
        return {"status": "Failed"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)