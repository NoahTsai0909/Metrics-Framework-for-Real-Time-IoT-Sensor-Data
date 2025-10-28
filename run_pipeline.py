import subprocess
import time
import threading
import os

# Paths to your scripts
SIMULATE_SCRIPT = "scripts/simulate_data.py"
ETL_SCRIPT = "scripts/etl.py"

def run_simulator():
    """Runs the simulator continuously."""
    print("Starting sensor data simulator...")
    subprocess.Popen(["python", SIMULATE_SCRIPT])

def run_etl_periodically(interval=10):
    """Runs the ETL cleaning script every N seconds."""
    print(f"Starting ETL cleaner (every {interval} seconds)...")
    while True:
        subprocess.run(["python", ETL_SCRIPT])
        print(f"ETL job finished. Waiting {interval} seconds...")
        time.sleep(interval)

def main():
    # Make sure data folders exist
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/clean", exist_ok=True)

    # Start simulator in a separate thread so it runs continuously
    simulator_thread = threading.Thread(target=run_simulator, daemon=True)
    simulator_thread.start()

    # Start ETL cleaning in the main thread
    run_etl_periodically(interval=10)

    time.sleep(60)

if __name__ == "__main__":
    main()
