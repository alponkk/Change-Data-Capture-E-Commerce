# Change Data Capture (CDC) Project

A complete end-to-end Change Data Capture pipeline that streams data from MongoDB through Kafka to ClickHouse, with dbt transformations for analytics.

## 🏗️ Data Architecture

This project implements a modern data architecture following the **Bronze → Silver → Gold** pattern:

### Data Flow Overview
```
MongoDB (Source) 
    ↓ [CDC]
Debezium Connector 
    ↓ [JSON Events]
Kafka Topics 
    ↓ [Stream Processing]
ClickHouse Sink Connector 
    ↓ [Raw JSON Storage]
ClickHouse Bronze Tables 
    ↓ [dbt Transformations]
ClickHouse Silver/Gold Tables
```

### Layer Architecture

**🥉 Bronze Layer (Raw Data)**
- **Purpose**: Store raw, unmodified data from source systems
- **Storage**: ClickHouse tables with JSON strings
- **Tables**: `mongo_ecom_customers`, `mongo_ecom_products`, `mongo_ecom_orders`
- **Data Format**: Raw Debezium CDC events in JSON format

**🥈 Silver Layer (Cleaned Data)**
- **Purpose**: Parsed, cleaned, and typed data ready for analytics
- **Storage**: ClickHouse tables with proper data types
- **Models**: `stg_orders`, `stg_customers`, `stg_products`
- **Transformations**: JSON parsing, data typing, field extraction

**🥇 Gold Layer (Business Metrics)**
- **Purpose**: Aggregated business metrics and KPIs
- **Storage**: ClickHouse fact and dimension tables
- **Models**: `fct_daily_sales`, `dim_customers`, `dim_products`, `fact_orders`
- **Transformations**: Business logic, aggregations, calculations

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Source Database** | MongoDB 5.0 | Operational data store with replica set |
| **Change Data Capture** | Debezium 2.1 | Captures database changes in real-time |
| **Message Bus** | Apache Kafka 7.3.0 | Streams CDC events reliably |
| **Stream Processing** | Kafka Connect | Moves data between systems |
| **Data Warehouse** | ClickHouse Latest | Columnar database for analytics |
| **Data Transformation** | dbt Core | SQL-based transformations |
| **Orchestration** | Docker Compose | Container orchestration |

## 📁 Project Structure

```
Change Data Capture Project/
├── README.md                             # Project documentation
├── docker-compose.yml                   # Docker services setup
├── requirements.txt                     # Python dependencies
├── config.template                      # Configuration template
│
├── 🔌 connectors/                        # Kafka Connect Configurations
│   ├── debezium-mongo-source-config.json # MongoDB CDC connector
│   └── clickhouse-sink-config.json      # ClickHouse sink connector
│
├── 🗄️ data-generation/                  # Data Generation Scripts
│   ├── seed_data.py                     # Generates fake e-commerce data
│   └── init-replica-set.js              # MongoDB replica set setup
│
├── 🛠️ scripts/                          # Utility Scripts
│   └── register_connectors.sh           # Deploy connectors script
│
├── 📚 docs/                             # Additional Documentation
│   └── [future documentation]
│
└── 📊 ecom_analytics/                   # dbt Analytics Project
    ├── dbt_project.yml                  # dbt project configuration
    ├── profiles.yml                     # dbt ClickHouse connection
    ├── packages.yml                     # dbt packages
    ├── models/
    │   ├── bronze_sources.yml           # Source table definitions
    │   ├── staging/
    │   │   ├── stg_customers.sql        # Customer staging model
    │   │   ├── stg_products.sql         # Product staging model  
    │   │   ├── stg_orders.sql           # Order staging model
    │   │   └── schema.yml               # Staging model tests
    │   └── marts/
    │       ├── dim_customers.sql        # Customer dimension
    │       ├── dim_products.sql         # Product dimension  
    │       ├── fact_orders.sql          # Detailed orders fact
    │       ├── fct_daily_sales.sql      # Daily sales aggregations
    │       └── schema.yml               # Marts model tests
    └── [other dbt directories]
```

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** - For running the infrastructure
- **Python 3.8+** - For data generation scripts
- **dbt Core & dbt-clickhouse** - For data transformations (or install via requirements.txt)

### 1. Start the Infrastructure

```bash
# Start all services (MongoDB, Kafka, ClickHouse, Kafka Connect)
docker-compose up -d

# Check service status
docker-compose ps
```

### 2. Install Python Dependencies

```bash
# Install all required Python packages
pip install -r requirements.txt
```

### 3. Initialize MongoDB and Generate Data

```bash
# Initialize MongoDB replica set (optional - auto-configured)
docker exec -it mongo mongosh --file /tmp/init-replica-set.js

# Generate fake e-commerce data
cd data-generation
python seed_data.py
cd ..
```

### 4. Deploy Change Data Capture Connectors

```bash
# Deploy Debezium source connector (ClickHouse sink requires manual setup)
cd scripts
chmod +x register_connectors.sh
./register_connectors.sh
cd ..
```

**Note**: The ClickHouse sink connector requires manual installation. For this demo, you can:
1. Use only the Debezium connector to stream to Kafka topics
2. Manually consume from Kafka and insert into ClickHouse
3. Or install the ClickHouse connector manually (see Advanced Setup section below)

### 5. Verify Data Flow

```bash
# Check Kafka topics are created
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Verify data is flowing to Kafka
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mongo.ecom.orders \
  --from-beginning \
  --max-messages 5

# Check ClickHouse is running
docker-compose exec clickhouse clickhouse-client -q "SELECT 1"
```

**Note**: Data will be in Kafka topics but not automatically in ClickHouse without the sink connector.

### 6. Run dbt Transformations (After Setting Up ClickHouse Sink)

```bash
cd ecom_analytics

# Install dbt packages
dbt deps

# Test dbt connection
dbt debug

# Run transformations (only works after ClickHouse sink is configured)
dbt run

# Run data quality tests
dbt test

cd ..
```

**Note**: dbt transformations require data in ClickHouse tables. Complete the ClickHouse connector setup first.

## 📊 Data Architecture Flow Diagram

Here's the detailed data flow through your pipeline:

```mermaid
graph TD
    A[MongoDB<br/>ecom Database] --> B[Debezium Connector]
    B --> C[Kafka Topics<br/>mongo.ecom.*]
    D[ClickHouse Sink<br/>Connector]
    
    subgraph "Data Sources"
        A1[Customers Collection]
        A2[Products Collection] 
        A3[Orders Collection]
    end
    
    subgraph "Kafka Topics"
        C1[mongo.ecom.customers]
        C2[mongo.ecom.products]
        C3[mongo.ecom.orders]
    end
    
    subgraph "ClickHouse Bronze"
        E1[mongo_ecom_customers]
        E2[mongo_ecom_products]
        E3[mongo_ecom_orders]
    end
    
    subgraph "ClickHouse Silver"
        G1[stg_customers]
        G2[stg_products]
        G3[stg_orders]
    end
    
    subgraph "ClickHouse Gold"
        H1[fct_daily_sales<br/>Daily Aggregations]
        H2[dim_customers<br/>Customer Dimension]
        H3[dim_products<br/>Product Dimension]
        H4[fact_orders<br/>Detailed Orders<br/>with Enrichments]
    end
    
    A1 --> A
    A2 --> A
    A3 --> A
    
    C --> C1
    C --> C2
    C --> C3
    
    C1 --> D
    C2 --> D
    C3 --> D
    
    D --> E1
    D --> E2
    D --> E3
    
    E1 --> G1
    E2 --> G2
    E3 --> G3
    
    G1 --> H2
    G2 --> H3
    G3 --> H1
    G3 --> H4
    H2 --> H4
    H3 --> H4
```

## 🔧 Configuration Details

### MongoDB Configuration
- **Replica Set**: `rs0` (required for Change Streams)
- **Port**: 27017
- **Database**: `ecom`
- **Collections**: `customers`, `products`, `orders`

### Kafka Configuration
- **Bootstrap Server**: `localhost:9092`
- **Zookeeper**: `localhost:2181`
- **Topic Prefix**: `mongo.ecom`
- **Replication Factor**: 1 (development)

### ClickHouse Configuration
- **HTTP Port**: 8123
- **Native Port**: 9000
- **Database**: `default`
- **User**: `default` (no password)

### dbt Configuration
- **Profile**: `clickhouse_project`
- **Target**: `dev`
- **Schema**: `default`

## 📈 Sample Queries

### Bronze Layer - Raw Data
```sql
-- View raw Debezium events
SELECT raw_data 
FROM mongo_ecom_orders 
LIMIT 1;
```

### Silver Layer - Parsed Data
```sql
-- View cleaned orders
SELECT order_id, customer_id, ordered_at, order_status
FROM stg_orders
LIMIT 10;
```

### Gold Layer - Business Metrics

#### Customer Dimension
```sql
-- Customer segmentation analysis
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(days_since_registration) as avg_days_registered
FROM dim_customers
GROUP BY customer_segment
ORDER BY customer_count DESC;
```

#### Product Dimension
```sql
-- Product inventory and pricing analysis
SELECT 
    price_category,
    inventory_status,
    COUNT(*) as product_count,
    AVG(price) as avg_price
FROM dim_products
GROUP BY price_category, inventory_status
ORDER BY price_category, inventory_status;
```

#### Comprehensive Order Facts
```sql
-- Detailed order analysis with customer and product info
SELECT 
    order_date,
    customer_name,
    customer_segment,
    product_name,
    price_category,
    quantity,
    unit_price,
    line_total,
    order_total_value,
    customer_lifetime_value
FROM fact_orders
WHERE order_date >= subtractDays(today(), 7)
ORDER BY order_total_value DESC
LIMIT 20;
```

#### Daily Sales Aggregations
```sql
-- Daily sales performance
SELECT 
    order_date,
    total_revenue,
    total_orders_placed,
    total_unique_customers,
    avg_order_value
FROM fct_daily_sales
ORDER BY order_date DESC
LIMIT 7;
```

## 🔍 Monitoring & Troubleshooting

### Check Connector Status
```bash
# List all connectors
curl http://localhost:8083/connectors

# Check specific connector status
curl http://localhost:8083/connectors/debezium-mongo-source/status
curl http://localhost:8083/connectors/clickhouse-ecom-sink/status
```

### View Kafka Messages
```bash
# Consume messages from orders topic
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mongo.ecom.orders \
  --from-beginning
```

### ClickHouse Queries
```bash
# Connect to ClickHouse
docker-compose exec clickhouse clickhouse-client

# Check table schemas
DESCRIBE TABLE mongo_ecom_orders;

# Monitor data ingestion
SELECT count(), max(_event_timestamp_ms) FROM stg_orders;
```

## 🎯 Use Cases

This comprehensive data pipeline enables real-time analytics for:

- **📊 Real-time Dashboards**: Live sales metrics and KPIs using `fct_daily_sales`
- **🔔 Alerting**: Anomaly detection on sales patterns and inventory levels
- **📈 Business Intelligence**: Historical trend analysis across customers, products, and orders
- **🎯 Personalization**: Customer segmentation and behavior analysis with `dim_customers`
- **📦 Inventory Management**: Product performance and stock tracking via `dim_products`
- **💰 Financial Reporting**: Detailed revenue analysis using `fact_orders` with enriched customer/product data
- **🛒 Order Analytics**: Comprehensive order analysis including price variance detection
- **👥 Customer Lifetime Value**: Track customer journey and purchasing patterns
- **🏷️ Product Performance**: Analyze product sales by category and pricing tiers

## 🔧 Advanced Setup: Manual ClickHouse Connector

The ClickHouse sink connector requires manual installation. Here are the options:

### Option 1: Manual Data Transfer (Recommended for Testing)
```bash
# Consume from Kafka and manually insert to ClickHouse
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mongo.ecom.orders \
  --from-beginning

# Then manually insert the JSON data into ClickHouse tables
```

### Option 2: HTTP Sink Connector (Alternative)
Use the HTTP sink connector to send data to ClickHouse HTTP interface:
```json
{
  "name": "http-clickhouse-sink",
  "config": {
    "connector.class": "io.confluent.connect.http.HttpSinkConnector",
    "topics.regex": "mongo\\.ecom\\..*",
    "http.api.url": "http://clickhouse:8123/",
    "request.method": "POST"
  }
}
```

### Option 3: Build Custom Connector
Download and build the ClickHouse connector from source:
```bash
# Clone and build the connector
git clone https://github.com/ClickHouse/clickhouse-kafka-connect.git
# Follow build instructions in their repository
```

## 🛡️ Production Considerations

For production deployment, consider:

- **Security**: Enable authentication, SSL/TLS encryption
- **Scalability**: Multi-node Kafka and ClickHouse clusters
- **Monitoring**: Prometheus + Grafana for observability
- **Backup**: Regular backups of ClickHouse data
- **Error Handling**: Dead letter queues and retry policies
- **Schema Evolution**: Proper versioning and migration strategies

## 📚 Additional Resources

- [Debezium Documentation](https://debezium.io/documentation/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Built with ❤️ for real-time data engineering** 