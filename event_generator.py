from datetime import datetime, timezone
import random
from uuid import uuid4

# Create the base event with a dictionary


def generate_event(sequence_no: int) -> dict:

    event_id = str(uuid4())
    event_time = datetime.now(timezone.utc).isoformat()

    sensors_list = {
        "sensor-temp-01": {
            "asset_id": "compressor-07",
            "metric_name": "temperature_c",
            "unit": "C",
            "normal_range": (40.0, 85.0),
        },
        "sensor-pressure-01": {
            "asset_id": "pump-03",
            "metric_name": "pressure_bar",
            "unit": "bar",
            "normal_range": (2.0, 8.0),
        },
        "sensor-vibration-01": {
            "asset_id": "turbine-02",
            "metric_name": "vibration_mm_s",
            "unit": "mm/s",
            "normal_range": (0.1, 4.5),
        },
    }

    sensor_id_random = random.choice(list(sensors_list.keys()))
    sensor_select = sensors_list[sensor_id_random]

    metric_value = round(random.uniform(*sensor_select["normal_range"]), 2)

    event = {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_source": "sensor-sim",
        "site_id": "plant-paris-01",
        "asset_id": sensor_select["asset_id"],
        "sensor_id": sensor_id_random,
        "metric_name": sensor_select["metric_name"],
        "metric_value": metric_value,
        "unit": sensor_select["unit"],
        "quality_code": "OK",
        "sequence_no": sequence_no,
        "firmware_version": "1.0.0",
    }

    return event


if __name__ == "__main__":
    print(generate_event(1))
