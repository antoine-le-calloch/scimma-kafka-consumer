import json
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta

from confluent_kafka import Consumer

SERVER_URL = "kafka.scimma.org"

TOPIC = "skyportal.skyportal" # Replace with the desired topic name
FROM_START = False  # Set to True to read from the beginning of the topic on each run (random consumer group)
MAX_AGE_DAYS = None  # Set to a number to skip messages older than that many days

# Credentials from https://scimma.org/hopauth/
USERNAME = "your_scimma_username"
PASSWORD = "your_scimma_password"

# -----------------------------------------------------------------------------


def build_consumer() -> Consumer:
    """Return a configured Confluent Kafka Consumer instance."""
    group_id = f"{USERNAME}-{TOPIC}2"
    if FROM_START: # If reading from the start, use a unique group ID to reset offsets.
        group_id += f"-{int(time.time())}"
    conf = {
        "bootstrap.servers": SERVER_URL,
        "group.id": group_id,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": False,
        "log_level": 2,
    }
    return Consumer(conf)


def print_metadata(consumer: Consumer) -> None:
    """Fetch and log metadata for the configured topic."""
    md = consumer.list_topics(TOPIC, timeout=2.0)
    for t in md.topics.values():
        logging.info("Topic: %s (partitions=%d)", t.topic, len(t.partitions))


def is_too_old(ts_ms: int, max_age: timedelta = timedelta(days=7)) -> bool:
    """Return True if the message timestamp is older than *max_age*.
    Use this to filter out old messages that are no longer relevant.
    """
    msg_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc) - msg_dt > max_age


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    consumer = build_consumer()
    print_metadata(consumer)

    consumer.subscribe([TOPIC])
    logging.info("Subscribed to %s", TOPIC)

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue

            if msg.error():
                logging.error("---\nKafka error: %s", msg.error())
                continue

            ts = msg.timestamp()[1]
            if MAX_AGE_DAYS is not None and ts > 0 and is_too_old(msg.timestamp()[1], timedelta(days=MAX_AGE_DAYS)):
                continue

            logging.info("---")
            logging.info("Offset %d",msg.offset())
            payload = msg.value()
            if not payload:
                logging.warning("Empty payload")
                continue

            try:
                data = json.loads(payload)
            except json.JSONDecodeError as exc:
                logging.error("JSON parse error: %s", exc)
                continue

            submitter = data.get("submitter")
            authors = data.get("authors")
            logging.info("Submitter(s): %s | Author(s): %s", submitter, authors)

            targets = data.get("data", {}).get("targets", [])
            for tgt in targets:
                logging.info("Target name: %s", tgt.get("name"))

            # At this point, 'data' contains the full decoded JSON payload.
            # You can insert your own logic here to store, transform, or forward the data as needed.

    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer.")
    parser.add_argument("--username", help="SCiMMA username")
    parser.add_argument("--password", help="SCiMMA password")
    parser.add_argument(
        "--from-start",
        action="store_true",
        help=(
            "If true, start reading the topic from the beginning (use a unique consumer group). "
            "If false, resume reading from the last committed offset for this consumer group "
            "(i.e., resume reading from where the script last stopped when run without this flag)."
        ),
    )
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=None,
        help="Skip messages older than this number of days (default: no limit)",
    )
    args = parser.parse_args()

    USERNAME = args.username if args.username else USERNAME
    PASSWORD = args.password if args.password else PASSWORD
    FROM_START = args.from_start if args.from_start else FROM_START
    MAX_AGE_DAYS = args.max_age_days if args.max_age_days else MAX_AGE_DAYS

    main()