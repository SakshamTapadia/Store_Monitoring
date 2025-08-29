from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse
import uuid
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import pytz
import uvicorn
from typing import Dict, List, Tuple

app = FastAPI()
DB_FILE = "store_monitoring.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute("""CREATE TABLE IF NOT EXISTS store_status(
        store_id TEXT, 
        timestamp_utc TEXT, 
        status TEXT
    )""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS business_hours(
        store_id TEXT, 
        day_of_week INTEGER, 
        start_time_local TEXT, 
        end_time_local TEXT
    )""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS store_timezone(
        store_id TEXT, 
        timezone_str TEXT
    )""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS report_status(
        report_id TEXT PRIMARY KEY, 
        status TEXT, 
        csv_path TEXT
    )""")
    
    conn.commit()
    conn.close()

def ingest_csvs():
    """Ingest CSV files into database"""
    conn = sqlite3.connect(DB_FILE)
    
    conn.execute("DELETE FROM store_status")
    conn.execute("DELETE FROM business_hours") 
    conn.execute("DELETE FROM store_timezone")
    
    try:
        status_df = pd.read_csv("store_status.csv")
        status_df.to_sql("store_status", conn, if_exists='append', index=False)
        
        hours_df = pd.read_csv("menu_hours.csv")
        hours_df = hours_df.rename(columns={
            'dayOfWeek': 'day_of_week',
            'start_time_local': 'start_time_local', 
            'end_time_local': 'end_time_local'
        })
        hours_df.to_sql("business_hours", conn, if_exists='append', index=False)
        
        tz_df = pd.read_csv("timezones.csv")
        tz_df.to_sql("store_timezone", conn, if_exists='append', index=False)
        
    except Exception as e:
        print(f"Error ingesting data: {e}")
    finally:
        conn.close()

def get_store_timezone(store_id: str, conn) -> str:
    """Get timezone for store, default to America/Chicago"""
    cursor = conn.cursor()
    cursor.execute("SELECT timezone_str FROM store_timezone WHERE store_id = ?", (store_id,))
    result = cursor.fetchone()
    return result[0] if result else "America/Chicago"

def get_business_hours(store_id: str, conn) -> Dict[int, Tuple[str, str]]:
    """Get business hours for store, default to 24/7"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT day_of_week, start_time_local, end_time_local 
        FROM business_hours 
        WHERE store_id = ?
    """, (store_id,))
    
    hours = {}
    for row in cursor.fetchall():
        day, start, end = row
        hours[day] = (start, end)
    
    if not hours:
        for day in range(7):
            hours[day] = ("00:00:00", "23:59:59")
    
    return hours

def get_store_observations(store_id: str, conn) -> List[Tuple[datetime, str]]:
    """Get all observations for a store"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp_utc, status 
        FROM store_status 
        WHERE store_id = ? 
        ORDER BY timestamp_utc
    """, (store_id,))
    
    observations = []
    for row in cursor.fetchall():
        timestamp_str, status = row
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        observations.append((timestamp, status))
    
    return observations

def is_within_business_hours(dt_local: datetime, business_hours: Dict[int, Tuple[str, str]]) -> bool:
    """Check if datetime is within business hours"""
    day_of_week = dt_local.weekday()
    
    if day_of_week not in business_hours:
        return False
    
    start_time_str, end_time_str = business_hours[day_of_week]
    
    start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()
    end_time = datetime.strptime(end_time_str, "%H:%M:%S").time()
    
    current_time = dt_local.time()
    
    if start_time <= end_time:
        return start_time <= current_time <= end_time
    else:
        return current_time >= start_time or current_time <= end_time

def compute_store_metrics(store_id: str, current_utc: datetime, conn) -> List[float]:
    """Compute uptime/downtime metrics for a store"""
    
    timezone_str = get_store_timezone(store_id, conn)
    business_hours = get_business_hours(store_id, conn)
    observations = get_store_observations(store_id, conn)
    
    if not observations:
        return [0.0, 0.0, 0.0, 0.0, 0.0, 0.0] 
    
    tz = pytz.timezone(timezone_str)
    
    one_hour_ago = current_utc - timedelta(hours=1)
    one_day_ago = current_utc - timedelta(days=1)
    one_week_ago = current_utc - timedelta(weeks=1)
    
    metrics = []
    
    for period_start, period_name in [(one_hour_ago, "hour"), (one_day_ago, "day"), (one_week_ago, "week")]:
        period_observations = [
            (ts, status) for ts, status in observations 
            if period_start <= ts <= current_utc
        ]
        
        if not period_observations:
            metrics.extend([0.0, 0.0])
            continue
        
        total_business_minutes = 0
        uptime_minutes = 0
        
        current_dt = period_start
        while current_dt < current_utc:
            local_dt = current_dt.astimezone(tz)
            
            if is_within_business_hours(local_dt, business_hours):
                total_business_minutes += 1
                
                closest_obs = None
                for obs_time, obs_status in period_observations:
                    if obs_time <= current_dt:
                        closest_obs = obs_status
                
                if closest_obs == "active":
                    uptime_minutes += 1
            
            current_dt += timedelta(minutes=1)
        
            if period_name == "hour":
                uptime = uptime_minutes
            downtime = total_business_minutes - uptime_minutes
        else: 
            uptime = uptime_minutes / 60.0 
            downtime = (total_business_minutes - uptime_minutes) / 60.0
        
        metrics.extend([max(0, uptime), max(0, downtime)])
    
    return metrics

def generate_report(report_id: str):
    """Generate the store monitoring report"""
    try:
        conn = sqlite3.connect(DB_FILE)
        
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp_utc) FROM store_status")
        max_timestamp_str = cursor.fetchone()[0]
        current_utc = datetime.fromisoformat(max_timestamp_str.replace('Z', '+00:00'))
        
        cursor.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = [row[0] for row in cursor.fetchall()]
        
        report_rows = []
        for store_id in store_ids:
            metrics = compute_store_metrics(store_id, current_utc, conn)
            report_rows.append([store_id] + metrics)
        
        csv_path = f"report_{report_id}.csv"
        df = pd.DataFrame(report_rows, columns=[
            "store_id", 
            "uptime_last_hour", "uptime_last_day", "uptime_last_week",
            "downtime_last_hour", "downtime_last_day", "downtime_last_week"
        ])
        df.to_csv(csv_path, index=False)
        
        cursor.execute("""
            UPDATE report_status 
            SET status = ?, csv_path = ? 
            WHERE report_id = ?
        """, ("Complete", csv_path, report_id))
        conn.commit()
        
    except Exception as e:
        print(f"Error generating report: {e}")
        cursor.execute("""
            UPDATE report_status 
            SET status = ? 
            WHERE report_id = ?
        """, ("Failed", report_id))
        conn.commit()
    finally:
        conn.close()

@app.on_event("startup")
async def startup_event():
    """Initialize database and ingest data on startup"""
    init_db()
    ingest_csvs()

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    """Trigger report generation"""
    report_id = str(uuid.uuid4())
    
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        INSERT INTO report_status(report_id, status, csv_path) 
        VALUES (?, ?, ?)
    """, (report_id, "Running", ""))
    conn.commit()
    conn.close()
    
    background_tasks.add_task(generate_report, report_id)
    
    return {"report_id": report_id}

@app.get("/get_report")
async def get_report(report_id: str):
    """Get report status or download CSV"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT status, csv_path 
        FROM report_status 
        WHERE report_id = ?
    """, (report_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return {"error": "Report not found"}
    
    status, csv_path = result
    
    if status == "Running":
        return {"status": "Running"}
    elif status == "Complete" and csv_path and os.path.exists(csv_path):
        return FileResponse(
            csv_path, 
            media_type="text/csv", 
            filename=f"store_report_{report_id}.csv"
        )
    else:
        return {"status": "Failed"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)