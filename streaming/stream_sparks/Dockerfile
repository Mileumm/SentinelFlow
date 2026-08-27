FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && JAVA_REAL_PATH="$(readlink -f "$(command -v java)")" \
    && JAVA_REAL_HOME="$(dirname "$(dirname "$JAVA_REAL_PATH")")" \
    && ln -s "$JAVA_REAL_HOME" /opt/java-home \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "streaming.spark_stream"]