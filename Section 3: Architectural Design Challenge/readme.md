# Architecture Overview

![image](https://github.com/user-attachments/assets/f10e00d9-12f8-45a8-bbd3-8f542a0903c1)



## 1. Technical Specifications

### 1.1 Data Collection Layer

**Web Tracking Implementation:**
- **JavaScript Tracer Library**: Custom JavaScript SDK deployed on Wix comparison pages that captures:
  - Page views with session information
  - Chart interaction events (filtering, sorting, expanding features)
  - Affiliate link clicks with link ID, position, and context
  - User context (device type, location, referrer)


**Event Transportation:**
- **Apache Kafka**: Distributed streaming platform for reliable, high-throughput event ingestion
  - Multiple partitioned topics for different event types
  - Configured with appropriate retention policies
- **Schema Registry**:
- Maintains Avro schemas for all event types
  - Enforces backward compatibility for schema evolution
  - Provides clear documentation for data producers and consumers

### 1.2 Data Processing Layer

**Stream Processing:**
- **Apache Spark Streaming**: Real-time processing with 10-second micro-batches
  - Performs initial data validation and enrichment
  - Applies business rules for segmentation
  - Calculates real-time metrics for dashboards
- **User Segmentation Service**: 
  -Applies business-defined rules to categorize users
  - Integrates with existing user profile data
  - Maintains segmentation rules in a configurable service

**Real-time Storage:**
- **Redis/ElasticSearch**: 
  - For real-time analytics and dashboards
  - Maintains recent activity for immediate analysis
  - Optimized for quick lookups and aggregations

**Batch Processing:**
- **Scheduled ETL Processes**: Daily aggregations and transformations
  - Performs heavy transformations and complex joins
  - Creates aggregated views for reporting
  - Handles historical backfilling when needed

**Data Warehouse:**
- **Snowflake**: Cloud data warehouse for all processed event data
  - Separate databases for raw, transformed, and reporting data
  - Time-travel capabilities for debugging and auditing
  - Multi-cluster architecture for scaling compute and storage independently

### 1.3 Data Access Layer

**Data Quality:**
- **Great Expectations**: Framework for data validation
  - Validates data against expected patterns and thresholds
  - Generates data quality reports
  - Alerts on anomalies or drift in metrics

**Data Management:**
- **dbt (data build tool)**: Transforms raw data into analytics models
  - Maintains testing, documentationb for all transformations
  - Enables version control of transformation logic
  - Provides a central repository for business definitions

**Data Catalog:**
- **Amundsen/DataHub**: Metadata management and data discovery
  - Indexes all data assets with descriptions and ownership
  - Shows lineage from raw events to reporting tables
  - Enables search and discovery for analysts

**Data Marts:**
- Curated views specific to marketing needs
  - Conversion funnels by segment
  - Attribution models
  - Campaign effectiveness
- **Self-Service**: Pre-aggregated tables for exploration
  - Daily/weekly/monthly trend analysis
  - User cohort analysis
  - Feature usage patterns

### 1.4 Analytics Layer

**Dashboards:**
- **Real-time Dashboard**: Shows current activity and immediate trends
  - Recent conversion rates
  - Active users on comparison pages
  - Real-time click-through rates

**BI Tools:**
- **Looker/Tableau**: For business analysts to create custom reports
  - Self-service exploration of the data
  - Scheduled reporting capabilities
  - Shareable dashboards and insights

**Self-Service Analytics:**
- **Custom interfaces** built on top of data marts
  - Simplified analysis tools for non-technical users
  - Pre-built report templates
  - Exportable results for presentations
