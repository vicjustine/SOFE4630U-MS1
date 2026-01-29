import os
import json
from google.cloud import pubsub_v1

# ======= EDIT THIS =======
PROJECT_ID = "projectmilestone1-485800"
SUBSCRIPTION_ID = "labelsTopic-sub"
CREDENTIALS_PATH = "/Users/olade/SOFE4630U-MS1/design_part/projectmilestone1-485800-ae56d9590ed2.json"
# =========================

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Deserialize bytes -> string -> dict
        data_str = message.data.decode("utf-8")
        data_dict = json.loads(data_str)

        print("\nReceived record:")
        for k, v in data_dict.items():
            print(f"  {k}: {v}")

        # Acknowledge message so it won't be redelivered
        message.ack()
        print("Acknowledged.\n")

    except Exception as e:
        print(f"Error processing message: {e}")
        # You can choose not to ack to force re-delivery
        message.nack()

def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Listening for messages on: {subscription_path}")
    print("Press Ctrl+C to stop.\n")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped.")

if __name__ == "__main__":
    main()
