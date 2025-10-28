import pandas as pd
import numpy as np
from datetime import datetime
import os
from sqlalchemy import create_engine


RAW_PATH = "data/raw/stream.csv"
CLEAN_PATH = "data/clean/clean_sensor_data.csv"
LOG_PATH = "data/clean/validation_log.txt"
LAST_RUN_PATH = "data/clean/last_run.txt"

user = "postgres"
password = "12345Yh890909'."
host = "localhost"
port = 5432
database = "sensor_data"


os.makedirs("data/clean", exist_ok=True)

def get_last_processed_line():
    """Return the last processed row index."""
    if os.path.exists(LAST_RUN_PATH):
        with open(LAST_RUN_PATH, "r") as f:
            return int(f.read().strip())
    return 0

def save_last_processed_line(count):
    """Save how many rows have been processed."""
    with open(LAST_RUN_PATH, "w") as f:
        f.write(str(count))

def validate_and_clean(df: pd.DataFrame):
    """Cleans and validates sensor data."""
    log = []

    #Convert timestamp
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    except Exception as e:
        log.append(f"Timestamp parsing error: {e}")

    #Convert numeric fields explicitly
    numeric_cols = ["temperature", "humidity", "vibration"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    #Drop invalid timestamps
    before = len(df)
    df = df.dropna(subset=["timestamp"])
    log.append(f"Dropped {before - len(df)} rows with invalid timestamps.")

    #Validate temperature
    invalid_temp = (df["temperature"] < -50) | (df["temperature"] > 100)
    log.append(f"{invalid_temp.sum()} rows with invalid temperature.")
    df = df[~invalid_temp]

    #Validate humidity
    invalid_humidity = (df["humidity"] < 0) | (df["humidity"] > 100)
    log.append(f"{invalid_humidity.sum()} rows with invalid humidity.")
    df = df[~invalid_humidity]

    #Validate vibration
    df.loc[df["vibration"] < 0, "vibration"] = 0

    #Drop duplicates
    before = len(df)
    df = df.drop_duplicates()
    log.append(f"Dropped {before - len(df)} duplicate rows.")

    #Handle missing data
    df = df.fillna(method="ffill")

    return df, log

def save_to_db(df):
    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_url)
    df.to_sql("sensor_readings", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into PostgreSQL.")

def main():
    print("Starting ETL process...")

    if not os.path.exists(RAW_PATH):
        print("No raw data found.")
        return

    total_rows = sum(1 for _ in open(RAW_PATH)) - 1  #minus header
    last_row = get_last_processed_line()

    if total_rows <= last_row:
        print("No new data to process.")
        return

    #Load only new rows
    new_data = pd.read_csv(RAW_PATH, skiprows=range(1, last_row + 1))
    print(f"Loaded {len(new_data)} new rows from {RAW_PATH} (rows {last_row + 1}–{total_rows})")

    cleaned_df, validation_log = validate_and_clean(new_data)

    #Save cleaned data incrementally
    if os.path.exists(CLEAN_PATH):
        cleaned_df.to_csv(CLEAN_PATH, mode="a", header=False, index=False)
    else:
        cleaned_df.to_csv(CLEAN_PATH, index=False)

    #Append to validation log
    with open(LOG_PATH, "a") as f:
        f.write(f"\n--- Run at {datetime.now().isoformat()} ---\n")
        f.write("\n".join(validation_log))
        f.write(f"\nProcessed {len(cleaned_df)} new rows.\n")

    print(f"Appended cleaned data and logs successfully.")

    #Update tracker
    save_last_processed_line(total_rows)
    
    #Upload to db
    save_to_db(cleaned_df)

if __name__ == "__main__":
    main()
