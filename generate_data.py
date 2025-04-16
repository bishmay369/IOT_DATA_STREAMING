import asyncio
import time
import random
import json
import os
import uuid
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# --- Configuration ---
# IMPORTANT: Replace this with YOUR valid Azure Event Hubs connection string.
# Ensure it has 'Send' permissions for the specified Event Hub.
# Consider using environment variables for security:
# CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STRING")
# CONNECTION_STR = "YOUR_EVENT_HUB_CONNECTION_STRING" # <--- *** REPLACE THIS ***
CONNECTION_STR = "Endpoint=sb://esehpn12xkzgvgk4bl50nl.servicebus.windows.net/;SharedAccessKeyName=key_d6fabfa8-8587-42b4-b348-798c3a69116c;SharedAccessKey=PwU4xSvDoEzt6o/j3ZS6stIfuaa99LP4W+AEhEBL1do=;EntityPath=es_d5a05da6-bba8-4709-bee6-75b266d496d4"


# IMPORTANT: Replace this with the specific name of your Event Hub.
# Consider using environment variables:
# EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")
EVENT_HUB_NAME = "es_d5a05da6-bba8-4709-bee6-75b266d496d4" # <--- *** REPLACE THIS ***

# Simulation Parameters
DEVICE_IDS = [f"pump_{i:03d}" for i in range(1, 8)] # pump_001 to pump_007
DEVICE_LOCATIONS = {
    "pump_001": "Refinery Unit A",
    "pump_002": "Refinery Unit A",
    "pump_003": "Refinery Unit B - East Wing",
    "pump_004": "Refinery Unit B - West Wing",
    "pump_005": "Storage Tank Farm 1",
    "pump_006": "Storage Tank Farm 2",
    "pump_007": "Pipeline Station 3",
}
SEND_INTERVAL_MIN_SECONDS = 0.5 # Minimum seconds between sends
SEND_INTERVAL_MAX_SECONDS = 2.0 # Maximum seconds between sends
HIGH_TEMP_ANOMALY_THRESHOLD = 90.0
HIGH_PRESSURE_ANOMALY_THRESHOLD = 180.0
ANOMALY_PROBABILITY = 0.01 # 1% chance per reading for each type of anomaly

# --- Helper Function ---
def get_event_hub_name_from_connection_string(connection_string):
    """Tries to parse the EntityPath (Event Hub name) from a connection string if present."""
    try:
        parts = connection_string.strip(';').split(';')
        for part in parts:
            if part.lower().startswith("entitypath="):
                return part[len("entitypath="):]
    except Exception:
        pass # Ignore parsing errors
    return None

# --- Main Sending Logic ---
async def run_continuous_sender():
    """Connects to Event Hub and continuously sends simulated IoT data."""

    # Check configuration
    if not CONNECTION_STR or CONNECTION_STR == "YOUR_EVENT_HUB_CONNECTION_STRING":
        raise ValueError("EVENT_HUB_CONNECTION_STRING is not set or is still the placeholder.")

    # Use provided EVENT_HUB_NAME if set, otherwise try to parse from connection string
    event_hub_name_to_use = EVENT_HUB_NAME
    if not event_hub_name_to_use or event_hub_name_to_use == "your-event-hub-name":
        print("EVENT_HUB_NAME not explicitly set, attempting to parse from connection string...")
        parsed_name = get_event_hub_name_from_connection_string(CONNECTION_STR)
        if parsed_name:
            event_hub_name_to_use = parsed_name
            print(f"Using Event Hub name parsed from connection string: {event_hub_name_to_use}")
        else:
             raise ValueError("EVENT_HUB_NAME is not set and could not be parsed from the connection string.")

    # Create a producer client to send messages to the event hub.
    # Use `async with` for automatic cleanup (closing the client).
    print(f"Attempting to connect to Event Hub: {event_hub_name_to_use}")
    async with EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=event_hub_name_to_use
    ) as producer:

        print("Connection successful. Starting continuous data generation...")
        while True:
            try:
                # Pick a random device
                device_id = random.choice(DEVICE_IDS)
                location = DEVICE_LOCATIONS.get(device_id, "Unknown Site")

                # Simulate normal sensor readings
                temperature = round(random.uniform(40.0, 80.0) + (random.random() * 5 - 2.5), 2)
                pressure = round(random.uniform(120.0, 160.0) + (random.random() * 10 - 5), 2)
                flow_rate = round(random.uniform(50.0, 150.0) + (random.random() * 20 - 10), 1)
                is_running = random.choice([True, True, True, False]) # Higher chance of running

                # Introduce occasional anomalies
                anomaly_detected = False
                if random.random() < ANOMALY_PROBABILITY:
                    temperature = round(random.uniform(HIGH_TEMP_ANOMALY_THRESHOLD, HIGH_TEMP_ANOMALY_THRESHOLD + 20), 2)
                    print(f"*** Anomaly Generated: High Temp for {device_id} ({temperature}°C) ***")
                    anomaly_detected = True
                if random.random() < ANOMALY_PROBABILITY:
                    pressure = round(random.uniform(HIGH_PRESSURE_ANOMALY_THRESHOLD, HIGH_PRESSURE_ANOMALY_THRESHOLD + 30), 2)
                    print(f"*** Anomaly Generated: High Pressure for {device_id} ({pressure} psi) ***")
                    anomaly_detected = True

                # Get timestamp and event ID
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                event_id = str(uuid.uuid4())

                # Structure data as JSON
                event_data_dict = {
                    "eventId": event_id,
                    "deviceId": device_id,
                    "timestamp": timestamp,
                    "location": location,
                    "isRunning": is_running,
                    "anomalyDetected": anomaly_detected,
                    "telemetry": {
                        "temperatureC": temperature,
                        "pressurePsi": pressure,
                        "flowRateLpm": flow_rate # Liters per minute
                    }
                }
                event_data_str = json.dumps(event_data_dict)
                print(f"Sending: {event_data_str}") # Print the data being sent

                # Create a batch (more efficient even for one event)
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(event_data_str))

                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)

                # Wait for a random interval before sending the next message
                sleep_duration = random.uniform(SEND_INTERVAL_MIN_SECONDS, SEND_INTERVAL_MAX_SECONDS)
                await asyncio.sleep(sleep_duration)

            except KeyboardInterrupt:
                print("\nStopping data generator due to user interrupt (Ctrl+C).")
                break # Exit the while loop
            except Exception as e:
                print(f"An error occurred: {e}")
                print("Attempting to continue after a delay...")
                # Basic retry logic / pause before continuing
                await asyncio.sleep(10) # Wait 10 seconds after an error

    print("Producer client closed.")

# --- Script Execution ---
if __name__ == "__main__":
    print("IoT Data Generator started.")
    print("Press Ctrl+C to stop.")
    try:
        # Run the asynchronous function using asyncio's event loop
        asyncio.run(run_continuous_sender())
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as main_exception:
         print(f"A critical error stopped the script: {main_exception}")
    finally:
        print("Script finished.")