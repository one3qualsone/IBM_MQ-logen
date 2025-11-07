# IBM MQ Metrics Generator for Elastic Demo

Dockerised data generator for realistic IBM MQ queue metrics with ML anomaly scenarios.

## Quick Start

### 1. Create your `.env` file
```bash
cp .env.example .env
# Edit .env with your Elasticsearch details
```

### 2. Get your Elasticsearch API Key

In Kibana:
1. Go to **Management** / **Stack Management** / **Security** / **API Keys**
2. Click **Create API key**
3. Name it: `mq-demo-generator`
4. Set privileges (or leave blank for full access)
5. Copy the **Encoded** API key (base64 format)
6. Paste into `.env` file as `ES_API_KEY`


Run `./create_index.sh` to create the index required for generating logs.


### 3. Generate Historical Data (for ML training)

Edit `.env`:
```bash
MODE=backfill
BACKFILL_DAYS=30
```

Run:
```bash
docker-compose up --build
```

This will generate 30 days of historical data with the incident pattern built in.

### 4. Switch to Continuous Mode

After backfill completes, edit `.env`:
```bash
MODE=continuous
CONTINUOUS_SCENARIO=normal
CONTINUOUS_INTERVAL_SECONDS=60
```

Run:
```bash
docker-compose up --build
```

## Switching Scenarios During Demo

To trigger different anomaly scenarios during demo:

1. Stop the container: `docker-compose down`
2. Edit `.env` and change `CONTINUOUS_SCENARIO` to:
   - `normal` - Normal operations
   - `gradual_degradation` - Queue slowly filling
   - `critical_buildup` - Near capacity
   - `queue_full` - Complete outage
   - `spike` - Sudden traffic spike
   - `recovery` - Returning to normal
3. Restart: `docker-compose up`

## Available Scenarios

| Scenario | Description | Use Case |
|----------|-------------|----------|
| `normal` | Balanced processing, realistic daily patterns | Baseline operations |
| `gradual_degradation` | Queue depth slowly increasing, processing slowing | Early warning detection |
| `critical_buildup` | Queue approaching capacity | Predictive alerting |
| `queue_full` | Complete processing stall | Outage simulation |
| `spike` | Sudden traffic surge | Burst handling |
| `recovery` | Returning to normal after incident | Post-incident monitoring |

## Queue Definitions

The generator creates metrics for 5 realistic banking queues:

- `PAYMENT.REQUEST` (Critical) - Inbound payment requests
- `PAYMENT.RESPONSE` (Critical) - Outbound payment responses  
- `SWIFT.OUTBOUND` (High) - International payments
- `PAYMENT.ERROR` (Medium) - Failed transactions
- `ISO20022.TRANSFORM` (High) - Message transformation queue

## Data Structure

Generates documents matching IBM MQ Prometheus exporter format:
```json
{
  "@timestamp": "2025-11-06T14:30:00Z",
  "prometheus": {
    "labels": {
      "qmgr": "QMPAYMENTS01",
      "queue": "PAYMENT.REQUEST",
      "priority": "critical"
    },
    "metrics": {
      "ibmmq_queue_depth": 1247,
      "ibmmq_queue_max_depth": 5000,
      "ibmmq_queue_utilisation_pct": 24.94,
      "ibmmq_queue_input_rate": 87,
      "ibmmq_queue_output_rate": 82
    }
  }
}
```

## Logs

View live logs:
```bash
docker-compose logs -f
```

## Stopping
```bash
docker-compose down
```

## Troubleshooting

**Connection refused:**
- Check `ES_URL` includes `https://` and port if needed
- Verify API key has correct privileges

**No data appearing:**
- Check logs: `docker-compose logs -f`
- Verify index name in Kibana Dev Tools: `GET metrics-mq-demo/_count`

**Rate limiting:**
- Increase `CONTINUOUS_INTERVAL_SECONDS` in `.env`
- Reduce `BACKFILL_INTERVAL_MINUTES` frequency