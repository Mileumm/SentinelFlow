from event_generator import generate_event


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
    "sequence_no",
    "firmware_version",
}


def test_generate_event_contains_required_fields():
    event = generate_event(1)

    assert set(event.keys()) == REQUIRED_FIELDS


def test_generate_event_sequence_no():
    event = generate_event(42)

    assert event["sequence_no"] == 42


def test_generate_event_metric_value_is_number():
    event = generate_event(1)

    assert isinstance(event["metric_value"], (int, float))


def test_generate_event_quality_code_is_ok():
    event = generate_event(1)

    assert event["quality_code"] == "OK"


def test_generate_event_sensor_fields_are_consistent():
    event = generate_event(1)

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