import pandas as pd
import numpy as np
import datetime

sensors = ['A01', 'A02', 'A03']
n = 1000
data = []

for i in range(n):
    timestamp = datetime.datetime.now() - datetime.timedelta(seconds=(n - i))
    sensor = np.random.choice(sensors)
    temperature = np.random.normal(25, 2)
    humidity = np.random.uniform(30, 70)
    vibration = np.random.uniform(0.1, 1.0)
    data.append([sensor, timestamp, temperature, humidity, vibration])

df = pd.DataFrame(data, columns=['sensor_id', 'timestamp', 'temperature', 'humidity', 'vibration'])
df.to_csv('data/raw/sensor_data.csv', index=False)


import time, random, csv

sensors = ['A01', 'A02', 'A03']

with open('data/raw/stream.csv', 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['sensor_id', 'timestamp', 'temperature', 'humidity', 'vibration'])

    while True:
        sensor = random.choice(sensors)
        timestamp = datetime.datetime.now().isoformat()
        temperature = round(random.uniform(20, 30), 2)
        humidity = round(random.uniform(30, 60), 2)
        vibration = round(random.uniform(0.1, 1.0), 2)
        writer.writerow([sensor, timestamp, temperature, humidity, vibration])
        f.flush()
        print(f"Generated data for {sensor}")
        time.sleep(2)