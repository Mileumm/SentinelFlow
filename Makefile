PYTHON = python3
VENV_DIR = .venv

ifeq ($(OS),Windows_NT)
	VENV_PYTHON = $(VENV_DIR)/Scripts/python.exe
else
	VENV_PYTHON = $(VENV_DIR)/bin/python
endif

COMPOSE = docker compose

KAFKA_CONTAINER = sentinel-kafka
KAFKA_BOOTSTRAP = kafka:29092
TOPIC = sensor-events
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION = 1

COUNT = 30
INTERVAL = 0.3
SCENARIO = mixed
OUTPUT = kafka
group_id = group_test_2

.PHONY: help venv install test run run-normal run-anomaly run-mixed docker-up docker-down docker-logs kafka-create-topic kafka-list-topics kafka-describe-topic producer-docker clean

help:
	@echo "Available commands:"
	@echo "  make venv                  Create Python virtual environment"
	@echo "  make install               Install Python dependencies"
	@echo "  make test                  Run pytest"
	@echo "  make run                   Run local producer with default scenario"
	@echo "  make run-normal            Run local producer with normal scenario"
	@echo "  make run-anomaly           Run local producer with anomaly scenario"
	@echo "  make run-mixed             Run local producer with mixed scenario"
	@echo "  make docker-up             Start Kafka and Kafka UI"
	@echo "  make docker-down           Stop Docker services"
	@echo "  make docker-logs           Show Docker logs"
	@echo "  make kafka-create-topic    Create Kafka topic explicitly"
	@echo "  make kafka-list-topics     List Kafka topics"
	@echo "  make kafka-describe-topic  Describe Kafka topic"
	@echo "  make producer-docker       Run producer inside Docker"
	@echo "  make producer-kafka        Run producer kafka"
	@echo "  make consumer-kafka        Run consumer kafka"
	@echo "  make consumer-sparks        Run consumer sparks"

venv:
	$(PYTHON) -m venv $(VENV_DIR)

install:
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

test:
	$(VENV_PYTHON) -m pytest -v

run:
	$(VENV_PYTHON) -m simulator.producer --count $(COUNT) --interval $(INTERVAL) --scenario $(SCENARIO)

run-normal:
	$(VENV_PYTHON) -m simulator.producer --count $(COUNT) --interval $(INTERVAL) --scenario normal

run-anomaly:
	$(VENV_PYTHON) -m simulator.producer --count $(COUNT) --interval $(INTERVAL) --scenario anomaly

run-mixed:
	$(VENV_PYTHON) -m simulator.producer --count $(COUNT) --interval $(INTERVAL) --scenario mixed

docker-up:
	$(COMPOSE) up -d kafka kafka-ui

docker-down:
	$(COMPOSE) down

docker-logs:
	$(COMPOSE) logs -f

kafka-create-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --create --if-not-exists --topic $(TOPIC) --partitions $(TOPIC_PARTITIONS) --replication-factor $(TOPIC_REPLICATION)

kafka-list-topics:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --list

kafka-describe-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --describe --topic $(TOPIC)

producer-docker:
	$(COMPOSE) run --rm producer --count $(COUNT) --interval $(INTERVAL) --scenario $(SCENARIO) --output console

producer-kafka:
	$(COMPOSE) run --rm producer --count $(COUNT) --interval $(INTERVAL) --scenario $(SCENARIO) --output $(OUTPUT)

consumer-kafka:
	$(COMPOSE) run --rm consumer --group_id $(group_id)

consumer-sparks:
	$(COMPOSE) run --rm spark_stream

clean:
	rm -rf .pytest_cache
	rm -rf **/__pycache__