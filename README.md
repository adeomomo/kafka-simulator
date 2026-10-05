# BESS Kafka Producer

A Docker-based simulator that produces realistic Battery Energy Storage System (BESS) sensor data to Apache Kafka. Simulates 30 BESS units generating voltage, current, temperature, and state of charge (SOC) data every 5 seconds.

## Overview

This project demonstrates:
- **KRaft Mode Kafka**: Modern Kafka deployment without Zookeeper
- **Python Kafka Producer**: Simulates multiple BESS units
- **Docker Compose**: Easy containerization and orchestration
- **Realistic Data**: Voltage (200-600V), Current (-100 to +100A), Temperature (15-45°C), SOC (0-100%)

## Prerequisites

- **Docker Desktop for Mac** (or Docker Engine on Linux)
- Ensure Docker is running before starting containers

## Project Structure

```
your-project-directory/
├── docker-compose.yml      # Kafka + Producer configuration
├── Dockerfile              # Python producer container definition
├── bess_producer.py        # Main producer script
└── README.md               # This file
```

## Quick Start

### 1. Start All Services in Background

```bash
docker compose up -d
```

The `-d` flag starts services in detached mode (runs in background).

### 2. Verify Services Are Running

```bash
docker compose ps
```

Expected output:
```
NAME                COMMAND                  SERVICE             STATUS
kafka               "/etc/confluent/dock…"   kafka               Up 2 seconds
bess-producer       "python bess_producer…"  bess-producer       Up 1 second
```

### 3. View Producer Logs

View logs in real-time without attaching to the container:

```bash
docker compose logs -f bess-producer
```

Press `Ctrl+C` to stop viewing logs (container keeps running).

Sample output:
```
2026-10-05 14:23:45,123 - INFO - Starting BESS Producer with 30 units
2026-10-05 14:23:45,456 - INFO - Successfully connected to Kafka at kafka:29092
2026-10-05 14:23:45,789 - INFO - Sent message for BESS-001: V=450.75V, I=32.45A, T=28.30°C, SOC=85.50%
2026-10-05 14:23:45,890 - INFO - Sent message for BESS-002: V=380.20V, I=-15.60A, T=22.15°C, SOC=92.30%
...
```

### 4. Consume Messages from Kafka

In another terminal, view all messages being produced:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bess-data --from-beginning
```

Sample message output:
```json
{
  "id": "BESS-001",
  "timestamp": "2026-10-05T14:23:45.123456Z",
  "voltage_v": 450.75,
  "current_a": 32.45,
  "temperature_c": 28.30,
  "soc_percent": 85.50
}
{
  "id": "BESS-002",
  "timestamp": "2026-10-05T14:23:45.234567Z",
  "voltage_v": 380.20,
  "current_a": -15.60,
  "temperature_c": 22.15,
  "soc_percent": 92.30
}
```

Press `Ctrl+C` to stop consuming.

## Common Commands

### View Kafka Logs

```bash
docker compose logs -f kafka
```

### View All Logs

```bash
docker compose logs -f
```

Press `Ctrl+C` to stop viewing.

### List Kafka Topics

```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Create a New Topic (Optional)

```bash
docker compose exec kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Describe the BESS Data Topic

```bash
docker compose exec kafka kafka-topics --describe --topic bess-data --bootstrap-server localhost:9092
```

### Stop Services (Keep Containers)

```bash
docker compose stop
```

### Restart Services

```bash
docker compose start
```

### Stop and Remove All Containers

```bash
docker compose down
```

### Remove All Data and Start Fresh

```bash
docker compose down -v
docker compose up -d
```

## Message Format

Each BESS message contains:

```json
{
  "id": "BESS-001",              // Unit ID (BESS-001 to BESS-030)
  "timestamp": "2026-10-05T14:23:45.123456Z",  // ISO 8601 UTC timestamp
  "voltage_v": 450.75,           // Voltage in volts (200-600V)
  "current_a": 32.45,            // Current in amps (-100 to +100A)
  "temperature_c": 28.30,        // Temperature in Celsius (15-45°C)
  "soc_percent": 85.50           // State of Charge (0-100%)
}
```

### Field Descriptions

| Field | Range | Description |
|-------|-------|-------------|
| `id` | BESS-001 to BESS-030 | Unique identifier for each BESS unit |
| `timestamp` | ISO 8601 UTC | When the measurement was taken |
| `voltage_v` | 200-600V | Battery system voltage |
| `current_a` | -100 to +100A | Current flow (negative = discharging, positive = charging) |
| `temperature_c` | 15-45°C | Operating temperature |
| `soc_percent` | 0-100% | State of Charge (battery capacity level) |

## Configuration

### Message Frequency

To change how often messages are produced, edit `bess_producer.py`:

```python
# Default: 5 seconds
producer.start_producing(interval_seconds=5)

# Change to 10 seconds
producer.start_producing(interval_seconds=10)
```

Rebuild the container:
```bash
docker compose up -d --build
```

### Number of BESS Units

To change the number of simulated units, edit `docker-compose.yml`:

```yaml
bess-producer:
  # Change environment variable
  environment:
    NUM_BESS_UNITS: 50  # Default is 30
```

Or edit `bess_producer.py`:

```python
# Default: 30 units
producer = BESSProducer(bootstrap_servers='kafka:29092', num_units=50)
```

### Kafka Broker Address

The producer connects to Kafka at `kafka:29092` (internal Docker network).
To connect from outside Docker, use `localhost:9092`.

## Troubleshooting

### Producer not connecting to Kafka

```bash
# Check if Kafka is running
docker compose ps

# View Kafka logs
docker compose logs kafka

# Ensure Kafka is fully started (may take 5-10 seconds)
```

### No messages in topic

```bash
# Check if topic was created
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
docker compose exec kafka kafka-topics --describe --topic bess-data --bootstrap-server localhost:9092
```

### Container keeps restarting

```bash
docker compose logs bess-producer

# Restart the service
docker compose restart bess-producer
```

### How to Detach from Container

If you're viewing logs with `docker compose logs -f`:
- Press `Ctrl+C` to stop viewing (container keeps running)

If you're in an interactive shell with `docker compose exec`:
- Press `Ctrl+P` then `Ctrl+Q` (container keeps running)

## Architecture

```
┌─────────────────────────────────────────┐
│        Docker Network (bess-network)    │
├─────────────────────────────────────────┤
│                                         │
│  ┌─────────────────────────────┐       │
│  │   Kafka (KRaft Mode)        │       │
│  │   - Port: 9092 (external)   │       │
│  │   - Port: 29092 (internal)  │       │
│  └─────────────────────────────┘       │
│           ↑                             │
│           │ (produces to)              │
│           │                             │
│  ┌─────────────────────────────┐       │
│  │   BESS Producer             │       │
│  │   - Simulates 30 units      │       │
│  │   - Sends every 5 seconds   │       │
│  └─────────────────────────────┘       │
│                                         │
└─────────────────────────────────────────┘
         │
         │ (accessible from host)
         ├─ localhost:9092 (Kafka)
```

## Technologies

- **Kafka 7.5.0** - Event streaming platform (KRaft mode)
- **Python 3.11** - Producer application
- **kafka-python** - Python Kafka client library
- **Docker** - Containerization
- **Docker Compose** - Multi-container orchestration

## Performance Notes

- **Message Volume**: 30 units × 1 message per 5 seconds = 6 messages/sec
- **Kafka Partitions**: 3 (can be adjusted)
- **Replication Factor**: 1 (single broker)

For production, consider:
- Multiple Kafka brokers
- Higher replication factor
- Persistent volumes
- Monitoring and alerting

## Next Steps

1. **Build a Consumer**: Create a service that consumes BESS data and stores it in a database
2. **Add Monitoring**: Use Prometheus/Grafana to visualize BESS metrics
3. **Real Data**: Replace random data with real sensor connections
4. **Scale Up**: Add more BESS units or multiple producer instances

## License

MIT

## Support

For issues or questions, check:
- Kafka logs: `docker compose logs kafka`
- Producer logs: `docker compose logs bess-producer`
- Docker status: `docker compose ps`
