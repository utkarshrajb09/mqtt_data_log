import time
import subprocess

while True:
    try:
        process = subprocess.Popen(["python", "10min_restart.py"])
        process.wait()
        print("Script exited, restarting...")
    except Exception as e:
        print(f"Error: {e}")

    # Sleep for a while before checking again
    # this sleep time for interval at which this script tried to restart main script
    time.sleep(60)