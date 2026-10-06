# Kafka Simulator - BESS Producer with MirrorMaker

A comprehensive Docker-based setup that simulates Battery Energy Storage System (BESS) sensor data production to Apache Kafka with **multi-cluster replication** using Kafka MirrorMaker. This project demonstrates modern Kafka architecture with KRaft mode, data mirroring, and realistic sensor simulation.

## Features

- **Dual Kafka Clusters (KRaft Mode)**: Source and target clusters running in Kraft mode without Zookeeper
- **BESS Producer**: Simulates 30 Battery Energy Storage units generating realistic sensor data
- **Kafka MirrorMaker 2.0**: Automatic topic and data replication from source to target cluster
- **Docker Compose**: Complete multi-container orchestration
- **Realistic Sensor Data**: Voltage (200-600V), Current (-100 to +100A), Temperature (15-45°C), SOC (0-100%)

## Project Structure

```
kafka-simulator/
├── docker-compose.yml              # Main Kafka clusters + services configuration
├── Dockerfile                       # BESS Producer container definition
├── Dockerfile.mirrormaker           # Kafka MirrorMaker container definition
├── bess_producer.py                 # BESS data producer script
├── mm2.properties                   # MirrorMaker configuration
├── source-cluster.properties        # Source cluster configuration
├── target-cluster.properties        # Target cluster configuration
├── connect-log4j.properties         # Logging configuration for connectors
├── docker-compose.yaml.original     # Backup of original compose file
├── restart.sh                       # Utility script to restart services
└── README.md                        # This file
```

## Prerequisites

- **Docker Desktop** (Mac/Windows) or **Docker Engine** (Linux)
- **Docker Compose** v1.29+
- Ensure Docker daemon is running before starting containers

## Quick Start

### 1. Start All Services

```bash
docker compose up -d
```

This starts:
- **kafka**: Source cluster (ports 9092, 9093)
- **kafka-2**: Target cluster (ports 9094, 9095)
- **bess-producer**: BESS data producer
- **mirrormaker**: Replicates data from source to target cluster

### 2. Verify All Services Are Running

```bash
docker compose ps
```

Expected output:
```
NAME                COMMAND                  SERVICE             STATUS
kafka               "/etc/confluent/dock…"   kafka               Up 2 seconds
kafka-2             "/etc/confluent/dock…"   kafka-2             Up 2 seconds
bess-producer       "python bess_producer…"  bess-producer       Up 1 second
mirrormaker         "/etc/confluent/dock…"   mirrormaker         Up 1 second
```

### 3. View Producer Logs

```bash
docker compose logs -f bess-producer
```

Sample output:
```
2026-10-05 14:23:45,123 - INFO - Starting BESS Producer with 30 units
2026-10-05 14:23:45,456 - INFO - Successfully connected to Kafka at kafka:29092
2026-10-05 14:23:45,789 - INFO - Sent message for BESS-001: V=450.75V, I=32.45A, T=28.30°C, SOC=85.50%
2026-10-05 14:23:45,890 - INFO - Sent message for BESS-002: V=380.20V, I=-15.60A, T=22.15°C, SOC=92.30%
```

### 4. Consume Messages from Source Cluster

View messages being produced to the source cluster:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bess-data --from-beginning
```

### 5. Consume Messages from Target Cluster (Mirrored)

View replicated messages on the target cluster:

```bash
docker compose exec kafka-2 kafka-console-consumer --bootstrap-server localhost:9094 --topic bess-data --from-beginning
```

Sample message format:
```json
{
  "id": "BESS-001",
  "timestamp": "2026-10-05T14:23:45.123456Z",
  "voltage_v": 450.75,
  "current_a": 32.45,
  "temperature_c": 28.30,
  "soc_percent": 85.50
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Docker Network (bess-network)            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────────┐          ┌──────────────────────┐ │
│  │  Kafka Source        │          │  Kafka Target        │ │
│  │  (KRaft Mode)        │          │  (KRaft Mode)        │ │
│  │  - 9092 (ext)        │          │  - 9094 (ext)        │ │
│  │  - 29092 (int)       │          │  - 29092 (int)       │ │
│  └──────────────────────┘          └──────────────────────┘ │
│           ▲                                 ▲                │
│           │                                 │                │
│      (produces)                        (replicates)         │
│           │                                 │                │
│           │                          ┌──────────────┐        │
│           └──────────────────────────│ MirrorMaker  │        │
│                                      │  (mm2)       │        │
│                                      └──────────────┘        │
│           │                                                  │
│  ┌────────┴─────────────┐                                   │
│  │  BESS Producer       │                                   │
│  │  - 30 units          │                                   │
│  │  - Every 5 seconds   │                                   │
│  └──────────────────────┘                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
        │
        ├─ Source: localhost:9092 (accessible from host)
        └─ Target: localhost:9094 (accessible from host)
```

## Common Commands

### View Kafka Logs

```bash
# Source cluster
docker compose logs -f kafka

# Target cluster
docker compose logs -f kafka-2

# MirrorMaker
docker compose logs -f mirrormaker

# All services
docker compose logs -f
```

### Kafka Topic Management

```bash
# List topics on source cluster
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe bess-data topic on source
docker compose exec kafka kafka-topics --describe --topic bess-data --bootstrap-server localhost:9092

# Check topics on target cluster (should be mirrored)
docker compose exec kafka-2 kafka-topics --list --bootstrap-server localhost:9094

# Create a test topic (optional)
docker compose exec kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Service Management

```bash
# Stop services (keep containers and data)
docker compose stop

# Restart services
docker compose start

# Stop and remove all containers
docker compose down

# Remove all data and start fresh
docker compose down -v
docker compose up -d

# Restart using the provided script
bash restart.sh
```

## BESS Data Format

Each message produced contains the following fields:

```json
{
  "id": "BESS-001",                              // Unit ID (BESS-001 to BESS-030)
  "timestamp": "2026-10-05T14:23:45.123456Z",   // ISO 8601 UTC timestamp
  "voltage_v": 450.75,                           // Voltage in volts (200-600V)
  "current_a": 32.45,                            // Current in amps (-100 to +100A)
  "temperature_c": 28.30,                        // Temperature in Celsius (15-45°C)
  "soc_percent": 85.50                           // State of Charge (0-100%)
}
```

### Field Reference

| Field | Range | Description |
|-------|-------|-------------|
| `id` | BESS-001 to BESS-030 | Unique identifier for each BESS unit |
| `timestamp` | ISO 8601 UTC | When the measurement was taken |
| `voltage_v` | 200-600V | Battery system voltage |
| `current_a` | -100 to +100A | Current flow (negative = discharging, positive = charging) |
| `temperature_c` | 15-45°C | Operating temperature |
| `soc_percent` | 0-100% | State of Charge (battery capacity level) |

## Configuration

### Producer Settings

Edit `bess_producer.py` to modify:

```python
# Number of BESS units to simulate
producer = BESSProducer(bootstrap_servers='kafka:29092', num_units=30)

# Message frequency (in seconds)
producer.start_producing(interval_seconds=5)
```

Rebuild the container after changes:
```bash
docker compose up -d --build
```

### MirrorMaker Configuration

Edit `mm2.properties` to modify replication settings:

```ini
# Source and target clusters
source.bootstrap.servers = kafka:29092
target.bootstrap.servers = kafka-2:29092

# Topics to replicate (comma-separated)
source->target.topics = bess-data

# Replication factor for internal topics
source->target.checkpoints.topic.replication.factor = 1
source->target.heartbeats.topic.replication.factor = 1
```

### Kafka Cluster Configuration

The docker-compose.yml defines:
- **Source Cluster (kafka)**: Ports 9092/29092, Node ID 1
- **Target Cluster (kafka-2)**: Ports 9094/29092, Node ID 2
- **KRaft Mode**: No Zookeeper required
- **Network**: Internal Docker network `bess-network`

## Troubleshooting

### Services Not Starting

```bash
# Check service status
docker compose ps

# View error logs
docker compose logs

# Restart all services
docker compose restart
```

### Producer Not Connecting to Kafka

```bash
# Check if Kafka is ready (wait 10-15 seconds)
docker compose logs kafka | grep "started"

# Verify network connectivity
docker compose exec bess-producer ping kafka

# Check Kafka bootstrap configuration
docker compose exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

### No Messages in Topic

```bash
# Check if topic was created
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# View topic details
docker compose exec kafka kafka-topics --describe --topic bess-data --bootstrap-server kafka:29092

# Check producer logs for errors
docker compose logs bess-producer
```

### MirrorMaker Not Replicating

```bash
# Check MirrorMaker status and logs
docker compose logs mirrormaker

# Verify source topic exists
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Verify target cluster is accessible
docker compose exec mirrormaker ping kafka-2
```

### Port Conflicts

If ports are already in use:
1. Stop conflicting services: `docker compose down`
2. Modify ports in `docker-compose.yml`
3. Restart: `docker compose up -d`

## Performance Characteristics

- **Message Rate**: 30 units × 1 message per 5 seconds = **6 messages/sec** (configurable)
- **Message Size**: ~150 bytes per message
- **Partitions**: 3 (configurable in producer)
- **Replication Factor**: 1 (single broker per cluster)
- **Consumer Lag**: Near real-time from source to target via MirrorMaker

### For Production Use

- Use multiple Kafka brokers per cluster
- Increase replication factor (typically 3)
- Enable SSL/TLS encryption
- Configure persistent volumes
- Add monitoring (Prometheus/Grafana)
- Implement proper security policies

## Technologies

| Component | Version | Purpose |
|-----------|---------|---------|
| **Kafka** | 7.5.0 | Event streaming platform (Confluent) |
| **KRaft Mode** | - | Kafka Raft consensus (no Zookeeper) |
| **MirrorMaker 2.0** | 7.5.0 | Multi-cluster replication |
| **Python** | 3.11 | Producer application |
| **kafka-python** | Latest | Python Kafka client library |
| **Docker** | Latest | Containerization |
| **Docker Compose** | v1.29+ | Orchestration |

## Next Steps

1. **Build a Consumer Service**: Create a service that consumes from target cluster and stores data
2. **Add Monitoring**: Integrate Prometheus/Grafana for metrics visualization
3. **Implement Real Data**: Replace random simulation with actual sensor data
4. **Scale Up**: Add more BESS units or multiple producer instances
5. **Multi-Region**: Deploy target clusters in different regions
6. **Data Pipeline**: Add stream processing (Kafka Streams, Flink, or Spark)

## License

MIT

## Support

For issues, check:

```bash
# Producer logs
docker compose logs bess-producer

# Source Kafka logs
docker compose logs kafka

# Target Kafka logs
docker compose logs kafka-2

# MirrorMaker logs
docker compose logs mirrormaker

# Service status
docker compose ps

# System status
docker stats
```
