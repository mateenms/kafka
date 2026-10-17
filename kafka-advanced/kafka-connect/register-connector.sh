#!/bin/bash
# Register the JDBC Sink Connector with Kafka Connect REST API
# Run this AFTER kafka-connect service is healthy (takes ~30s to start)

CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG="$(dirname "$0")/connectors/order-events-sink.json"

echo "Waiting for Kafka Connect to be ready..."
until curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka Connect is ready."

echo "Registering order-events JDBC sink connector..."
curl -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTOR_CONFIG}"

echo ""
echo "Connector registered. Check status:"
echo "  curl ${CONNECT_URL}/connectors/order-events-jdbc-sink/status"
