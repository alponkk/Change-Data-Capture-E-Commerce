#!/bin/bash

# Register Connectors Script
# This script registers the Debezium MongoDB source connector and ClickHouse sink connector
# to the Kafka Connect cluster via REST API

set -e  # Exit on any error

# Configuration
KAFKA_CONNECT_URL="http://localhost:8083"
DEBEZIUM_CONFIG_FILE="../connectors/debezium-mongo-source-config.json"
CLICKHOUSE_CONFIG_FILE="../connectors/clickhouse-sink-config.json"

echo "🚀 Starting connector registration process..."
echo "=" * 50

# Function to check if Kafka Connect is ready
check_kafka_connect() {
    echo "🔍 Checking if Kafka Connect is ready..."
    for i in {1..30}; do
        if curl -s -f "${KAFKA_CONNECT_URL}" > /dev/null 2>&1; then
            echo "✅ Kafka Connect is ready!"
            return 0
        fi
        echo "⏳ Waiting for Kafka Connect to be ready (attempt $i/30)..."
        sleep 5
    done
    echo "❌ Kafka Connect is not ready after 150 seconds"
    exit 1
}

# Function to register a connector
register_connector() {
    local config_file=$1
    local connector_name=$2
    
    echo "📤 Registering connector from: $config_file"
    
    # Check if config file exists
    if [ ! -f "$config_file" ]; then
        echo "❌ Configuration file $config_file not found!"
        exit 1
    fi
    
    # Register the connector
    response=$(curl -s -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d @"$config_file" \
        "${KAFKA_CONNECT_URL}/connectors")
    
    # Extract HTTP status code (last 3 characters)
    http_code="${response: -3}"
    response_body="${response%???}"
    
    if [ "$http_code" -eq 201 ] || [ "$http_code" -eq 200 ]; then
        echo "✅ Successfully registered connector: $connector_name"
        echo "📋 Response: $response_body"
    else
        echo "❌ Failed to register connector: $connector_name"
        echo "📋 HTTP Status: $http_code"
        echo "📋 Response: $response_body"
        exit 1
    fi
}

# Function to check connector status
check_connector_status() {
    local connector_name=$1
    echo "🔍 Checking status of connector: $connector_name"
    
    status_response=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${connector_name}/status")
    echo "📊 Status: $status_response"
    echo ""
}

# Main execution
main() {
    # Check if Kafka Connect is ready
    check_kafka_connect
    
    echo ""
    echo "📝 Starting connector registration..."
    echo ""
    
    # Register Debezium MongoDB Source Connector
    echo "1️⃣  Registering Debezium MongoDB Source Connector..."
    register_connector "$DEBEZIUM_CONFIG_FILE" "debezium-mongo-source"
    
    echo ""
    echo "⏳ Waiting 5 seconds before registering next connector..."
    sleep 5
    echo ""
    
    # Note: ClickHouse Sink Connector requires manual installation
    echo "2️⃣  ClickHouse Sink Connector skipped (requires manual installation)"
    echo "    📋 The ClickHouse connector needs to be installed manually."
    echo "    📋 For now, data will be available in Kafka topics: mongo.ecom.*"
    
    echo ""
    echo "🎉 Debezium connector registered successfully!"
    echo ""
    
    # Check status of Debezium connector
    echo "📊 Checking connector status..."
    echo ""
    check_connector_status "debezium-mongo-source"
    
    echo "✨ Connector registration process completed!"
    echo ""
    echo "💡 Useful commands:"
    echo "   • List all connectors: curl ${KAFKA_CONNECT_URL}/connectors"
    echo "   • Check connector status: curl ${KAFKA_CONNECT_URL}/connectors/{connector-name}/status"
    echo "   • Delete connector: curl -X DELETE ${KAFKA_CONNECT_URL}/connectors/{connector-name}"
}

# Run main function
main "$@" 