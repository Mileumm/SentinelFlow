from datetime import datetime, timezone
import random
from uuid import uuid4

# Create the base event with a dictionary

SENSORS = {
    "sensor-temp-01": {
        "asset_id": "compressor-07",
        "metric_name": "temperature_c",
        "unit": "C",
        "normal_range": (40.0, 85.0),
        "low_range": (0.0, 39.9),
        "high_range": (85.1, 100.0),
    },
    "sensor-pressure-01": {
        "asset_id": "pump-03",
        "metric_name": "pressure_bar",
        "unit": "bar",
        "normal_range": (2.0, 8.0),
        "low_range": (0.0, 1.9),
        "high_range": (8.1, 10.0),
    },
    "sensor-vibration-01": {
        "asset_id": "turbine-02",
        "metric_name": "vibration_mm_s",
        "unit": "mm/s",
        "normal_range": (0.1, 4.5),
        "low_range": (0.0, 0.09),
        "high_range": (4.51, 10.0),
    },
}

def generate_event(sequence_no: int, scenario:str = "normal") -> dict:

    if scenario == "mixed":
        if random.random() < 0.1:
            scenario = "anomaly"
        else:
            scenario = "normal"

    event_id = str(uuid4())
    event_time = datetime.now(timezone.utc).isoformat()

    sensor_id_random = random.choice(list(SENSORS.keys()))
    sensor_select = SENSORS[sensor_id_random]

    if scenario == "normal":
        metric_value = round(random.uniform(*sensor_select["normal_range"]), 2)
        quality_code = "OK"
        anomaly_type = None
    elif scenario == "anomaly":
        anomaly_type = random.choice(["low", "high"])
        if anomaly_type == "low":
            metric_value = round(random.uniform(*sensor_select["low_range"]), 2)
        else:
            metric_value = round(random.uniform(*sensor_select["high_range"]), 2)
        quality_code =  "WARN"
    else:
        raise ValueError("Invalid scenario. Choose 'normal' or 'anomaly'.")

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
        "quality_code": quality_code,
        "anomaly_type": anomaly_type,
        "sequence_no": sequence_no,
        "firmware_version": "1.0.0",
    }

    return event


if __name__ == "__main__":
    print(generate_event(1, "normal"))
    print(generate_event(2, "anomaly"))
