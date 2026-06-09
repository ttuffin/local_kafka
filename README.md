# Local Kafka (SASL_SSL)

A Podman Compose setup for running Apache Kafka locally with SASL/PLAIN authentication over SSL, a web-based management UI (kafka-ui), and a Caddy reverse proxy for HTTPS access.

## Architecture

- **Kafka** (`apache/kafka:latest`) -- Single-node KRaft broker with SASL_SSL on ports 9092 (host) and 29092 (inter-container).
- **Kafka UI** (`provectuslabs/kafka-ui:latest`) -- Web interface for inspecting topics, consumers, and messages. Accessed via Caddy.
- **Caddy** -- Reverse proxy serving Kafka UI over HTTPS on port 9443.

All services communicate over a shared Podman network (`docker_default`).

## Prerequisites

- Podman and Podman Compose (or whichever container runtime you prefer)
- `keytool` (ships with any JDK/JRE) -- for generating SSL certificates
- Python 3.8+ with `aiokafka` -- only needed if using the batch producer script

## Setup

### 1. Create the Podman network

The compose file expects an external network called `docker_default`. Create it if it doesn't already exist:

```bash
podman network create docker_default
```

### 2. Generate SSL certificates

Run the certificate generation script to create the broker keystore, truststore, and Caddy TLS certificate:

```bash
./generate-certs.sh
```

This populates the `certs/` directory with:

| File | Purpose |
|---|---|
| `kafka.broker.keystore.jks` | Broker identity (private key + cert) |
| `broker-cert.pem` | Exported broker certificate |
| `kafka.client.truststore.jks` | Client truststore containing the broker cert |

You also need to generate a TLS certificate for Caddy. For example, using `openssl`:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout certs/caddy.key -out certs/caddy.crt \
  -days 365 -subj "/CN=localhost"
```

### 3. Start the services

```bash
podman-compose -f kafka-compose.yml up -d
```

### 4. Access Kafka UI

Open [https://localhost:9443](https://localhost:9443) in your browser (accept the self-signed certificate warning).

Login credentials:

| Field | Value |
|---|---|
| Username | `admin` |
| Password | `admin-password` |

## Kafka Authentication

The broker is configured with SASL/PLAIN. Two users are defined in `kafka_server_jaas.conf`:

| Username | Password |
|---|---|
| `admin` | `admin-password` |
| `alice` | `alice-password` |

`admin` is used for inter-broker communication and Kafka UI. `alice` is available as a regular client user.

### Connecting from the host

Use `client.properties` with Kafka CLI tools:

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic test \
  --producer.config client.properties
```

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test --from-beginning \
  --consumer.config client.properties
```

### Connecting from another container

Use the internal listener at `kafka-sasl:29092` (the container must be on the `docker_default` network).

## Batch Producer

`batch_producer.py` is an async Python script for bulk-producing messages to Kafka using `aiokafka`.

### Install dependencies

```bash
pip install aiokafka
```

### Usage

```bash
python batch_producer.py <start> <end> [batch_size] [blob_kb_size]
```

| Argument | Description | Default |
|---|---|---|
| `start` | First message ID (inclusive) | *required* |
| `end` | Last message ID (exclusive) | *required* |
| `batch_size` | Messages per flush | 100 |
| `blob_kb_size` | Size of random data blob per message (KB) | 0 (no blob) |

Example -- send 1000 messages with 5 KB payloads, flushing every 100:

```bash
python batch_producer.py 0 1000 100 5
```

The script prints performance statistics (throughput, messages/second) on completion.

## Stopping

```bash
podman-compose -f kafka-compose.yml down
```

To also remove persisted data:

```bash
podman-compose -f kafka-compose.yml down -v
```
