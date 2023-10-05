import paho.mqtt.client as mqtt
import time
import pyodbc
import json
from datetime import datetime
import pytz
import os
import signal

# SQL Server Configuration
db_server = "LAPTOP-41OB4IUG\SQLEXPRESS"
db_name = "client_login"
db_user = "sa"
db_password = "abc@123"

# Connecting to the database
connection_string = f"DRIVER={{SQL Server}};SERVER={db_server};DATABASE={db_name};UID={db_user};PWD={db_password}"
db = pyodbc.connect(connection_string)
cursor = db.cursor()


# Initialize a flag and a timer
last_logged_minute = None
last_insert_time = time.time()

#
# def cleanup_data():
#     global last_logged_minute
#
#     # Calculate the timestamp of the last 10th minute
#     now = datetime.now(pytz.timezone("Asia/Kolkata"))
#     last_10th_minute = (now.minute // 10) * 10
#     last_10th_minute_time = now.replace(minute=last_10th_minute, second=0, microsecond=0)
#
#     # Format the timestamp for comparison with database records
#     last_10th_minute_str = last_10th_minute_time.strftime("%Y-%m-%d %H:%M:%S")
#
#     delete_query = "DELETE FROM mqtt_tags_data WHERE datetime_ist < ?"
#     cursor.execute(delete_query, (last_10th_minute_str,))
#     db.commit()
#
#     print(f"Deleted data older than {last_10th_minute_str}")


def on_message(client, userdata, message):
    global last_logged_minute, last_insert_time

    payload = str(message.payload.decode("utf-8"))
    try:
        mqtt_data = json.loads(payload)
        tags = mqtt_data.get("tags", {})
        timestamp_str = mqtt_data.get("timestamp", "")

        timestamp_utc = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp_utc = pytz.utc.localize(timestamp_utc)
        timestamp_ist = timestamp_utc.astimezone(pytz.timezone("Asia/Kolkata"))

        current_minute = timestamp_ist.minute

        if current_minute % 10 == 0 and current_minute != last_logged_minute:
            last_logged_minute = current_minute
            last_insert_time = time.time()

            date = timestamp_ist.strftime("%Y-%m-%d")
            time_24_hour = timestamp_ist.strftime("%H:%M:%S")
            datetime_ist = date + " " + time_24_hour

            # # Clean up data older than the last 10th minute
            # cleanup_data()

            for key, value in tags.items():
                print("Timestamp IST:", timestamp_ist)
                insert_query = "INSERT INTO mqtt_tags_data (tag_key, tag_value, date, time_24_hour, datetime_ist) VALUES (?, ?, ?, ?, ?)"
                cursor.execute(insert_query, (key, value, date, time_24_hour, datetime_ist))
                print("Data inserted:", key, value, date, time_24_hour, datetime_ist)
                db.commit()

    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"Unexpected disconnection. Reconnecting... (Error code: {rc})")

        timestamp_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
        date = timestamp_ist.strftime("%Y-%m-%d")
        time_24_hour = timestamp_ist.strftime("%H:%M:%S")
        datetime_ist = date + " " + time_24_hour

        # Log the disconnection status in the downtime table
        insert_downtime_query = "INSERT INTO downtime (timestamp, status) VALUES (?, ?)"
        cursor.execute(insert_downtime_query, (datetime_ist, "Disconnected"))
        db.commit()

        client.reconnect()


def check_inserted_data():
    global last_insert_time
    current_time = time.time()

    if current_time - last_insert_time > 900:  # 900 seconds = 15 minutes
        print("No data inserted for 15 minutes. Restarting...")
        os.kill(os.getpid(), signal.SIGTERM)


broker_address = "103.120.179.143"
client = mqtt.Client("P1")
client.on_message = on_message
client.on_disconnect = on_disconnect  # Assign the on_disconnect callback
client.connect(broker_address)
client.loop_start()
client.subscribe("DA30/B25")

try:
    while True:
        time.sleep(600)  # Sleep for 10 minutes
        check_inserted_data()
except KeyboardInterrupt:
    pass
