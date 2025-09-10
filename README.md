# Scimma kafka consumer

A simple Kafka consumer for reading messages from topics hosted on [SCiMMA Kafka](https://scimma.org).

## Usage

```bash
    python kafka_consumer.py --username your_scimma_username --password your_scimma_password
```

### Optional flags

        --username USERNAME – SCiMMA username
        --password PASSWORD – SCiMMA password
        --from-start – Read messages from the beginning of the topic
        --max-age-days N – Skip messages older than N days

Create credentials at https://scimma.org/hopauth/

### Note
This script is preconfigured to consume from the topic:

    skyportal.skyportal

You can change the topic by modifying the TOPIC variable in the script.

### Requirements

- Python 3.7+ 
- confluent-kafka

### Install dependencies:

```bash
  pip install -r requirements.txt
```
