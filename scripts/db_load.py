import pandas as pd
from sqlalchemy import create_engine

user = "postgres"
password = "12345Yh890909'."
host = "localhost"
port = 5432
database = "sensor_data"

connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_url)


# Load the cleaned CSV
df = pd.read_csv("data/clean/clean_sensor_data.csv")

# Write to PostgreSQL
df.to_sql("sensor_readings", engine, if_exists="append", index=False)

print(f"Inserted {len(df)} rows into PostgreSQL successfully!")