#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "Creating index mapping for: $INDEX_NAME"

curl -X PUT "${ES_URL}/${INDEX_NAME}" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index": {
      "lifecycle": {
        "name": "metrics-30-days"
      }
    }
  },
  "mappings": {
    "properties": {
      "@timestamp": {"type": "date"},
      "prometheus": {
        "properties": {
          "labels": {
            "properties": {
              "qmgr": {"type": "keyword"},
              "queue": {"type": "keyword"},
              "cluster": {"type": "keyword"},
              "priority": {"type": "keyword"}
            }
          },
          "metrics": {
            "properties": {
              "ibmmq_queue_depth": {"type": "long"},
              "ibmmq_queue_max_depth": {"type": "long"},
              "ibmmq_queue_input_count": {"type": "long"},
              "ibmmq_queue_output_count": {"type": "long"},
              "ibmmq_queue_input_rate": {"type": "long"},
              "ibmmq_queue_output_rate": {"type": "long"},
              "ibmmq_queue_oldest_message_age": {"type": "long"},
              "ibmmq_queue_utilisation_pct": {"type": "float"}
            }
          }
        }
      },
      "host": {
        "properties": {
          "name": {"type": "keyword"},
          "hostname": {"type": "keyword"}
        }
      },
      "event": {
        "properties": {
          "dataset": {"type": "keyword"},
          "module": {"type": "keyword"},
          "kind": {"type": "keyword"}
        }
      }
    }
  }
}'

echo -e "\n\n✅ Index created successfully"