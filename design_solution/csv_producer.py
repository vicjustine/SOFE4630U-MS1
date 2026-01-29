import os
import csv
import json
import time
from google.cloud import pubsub_v1

# ======= EDIT THIS =======
PROJECT_ID = "projectmilestone1-485800"
TOPIC_ID = "labelsTopic"
CREDENTIALS_PATH = "/Users/olade/SOFE4630U-MS1/design_part/projectmilestone1-485800-ae56d9590ed2.json"
CSV_FILE = "Labels.csv"
# =========================

def main():
    # Force correct credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing CSV rows to: {topic_path}")
    print(f"Reading CSV file: {CSV_FILE}\n")

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        count = 0
        for row in reader:
            # row is already a dict: {columnName: value, ...}
            message_dict = dict(row)

            # Serialize dict -> JSON string -> bytes
            message_bytes = json.dumps(message_dict).encode("utf-8")

            future = publisher.publish(topic_path, message_bytes)
            msg_id = future.result()

            count += 1
            print(f"[{count}] Published message_id={msg_id} | data={message_dict}")

            # Optional small delay so output is readable
            time.sleep(0.2)

    print(f"\nDone. Published {count} records.")

if __name__ == "__main__":
    main()
