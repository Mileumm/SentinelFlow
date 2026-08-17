from simulator.event_generator import generate_event
from simulator.event_generator import SENSORS
import simulator.event_generator as event_generator
import pytest

REQUIRED_FIELDS = {
    "event_id",
    "event_time",
    "ingest_source",
    "site_id",
    "asset_id",
    "sensor_id",
    "metric_name",
    "metric_value",
    "unit",
    "quality_code",
    "anomaly_type",
    "sequence_no",
    "firmware_version",
}


def test_generate_event_contains_required_fields():
    event = generate_event(1, "normal")
    assert set(event.keys()) == REQUIRED_FIELDS

    event = generate_event(1, "anomaly")
    assert set(event.keys()) == REQUIRED_FIELDS


def test_generate_event_sequence_no():
    event = generate_event(42, "normal")

    assert event["sequence_no"] == 42

    event = generate_event(42, "anomaly")

    assert event["sequence_no"] == 42

def test_generate_event_metric_value_is_number():
    event = generate_event(1, "normal")

    assert isinstance(event["metric_value"], (int, float))

    event = generate_event(1, "anomaly")

    assert isinstance(event["metric_value"], (int, float))

def test_generate_event_quality_code_is_ok():
    event = generate_event(1, "normal")

    assert event["quality_code"] == "OK"

    event = generate_event(1, "anomaly")

    assert event["quality_code"] == "WARN"

def test_generate_event_sensor_fields_are_consistent():
    event = generate_event(1, "normal")

    if event["sensor_id"] == "sensor-temp-01":
        assert event["asset_id"] == "compressor-07"
        assert event["metric_name"] == "temperature_c"
        assert event["unit"] == "C"

    elif event["sensor_id"] == "sensor-pressure-01":
        assert event["asset_id"] == "pump-03"
        assert event["metric_name"] == "pressure_bar"
        assert event["unit"] == "bar"

    elif event["sensor_id"] == "sensor-vibration-01":
        assert event["asset_id"] == "turbine-02"
        assert event["metric_name"] == "vibration_mm_s"
        assert event["unit"] == "mm/s"

    else:
        raise AssertionError(f"Unknown sensor_id: {event['sensor_id']}")

def test_generate_normal_event_anomaly_type_is_none():
    event = generate_event(1, "normal")

    assert event["anomaly_type"] is None


def test_generate_anomaly_event_anomaly_type_is_low_or_high():
    event = generate_event(1, "anomaly")

    assert event["anomaly_type"] in {"low", "high"}
    
def test_mixed_event_has_valid_quality_code_and_anomaly_type():
    event = generate_event(1, "mixed")

    assert event["quality_code"] in {"OK", "WARN"}

    if event["quality_code"] == "OK":
        assert event["anomaly_type"] is None

    elif event["quality_code"] == "WARN":
        assert event["anomaly_type"] in {"low", "high"}

def test_generate_event_invalid_scenario_raises_error():
    with pytest.raises(ValueError):
        generate_event(1, "invalid-scenario")
        
def test_normal_event_metric_value_is_inside_normal_range():
    for sequence_no in range(1, 101):
        event = generate_event(sequence_no, "normal")
        sensor = SENSORS[event["sensor_id"]]

        min_value, max_value = sensor["normal_range"]

        assert min_value <= event["metric_value"] <= max_value
        
def test_anomaly_event_metric_value_is_outside_normal_range():
    for sequence_no in range(1, 101):
        event = generate_event(sequence_no, "anomaly")
        sensor = SENSORS[event["sensor_id"]]

        if event["anomaly_type"] == "low":
            min_value, max_value = sensor["low_range"]

        elif event["anomaly_type"] == "high":
            min_value, max_value = sensor["high_range"]

        else:
            raise AssertionError(f"Invalid anomaly_type: {event['anomaly_type']}")

        assert min_value <= event["metric_value"] <= max_value
        
def test_mixed_can_generate_normal_event(monkeypatch):
    monkeypatch.setattr(event_generator.random, "random", lambda: 0.5)

    event = event_generator.generate_event(1, "mixed")

    assert event["quality_code"] == "OK"
    assert event["anomaly_type"] is None


def test_mixed_can_generate_anomaly_event(monkeypatch):
    monkeypatch.setattr(event_generator.random, "random", lambda: 0.05)

    event = event_generator.generate_event(1, "mixed")

    assert event["quality_code"] == "WARN"
    assert event["anomaly_type"] in {"low", "high"}