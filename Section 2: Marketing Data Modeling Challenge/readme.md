# Data Model Design Rationale
The dimensional model presented in the ERD follows a star schema design with a central fact table (FACT_AD_PERFORMANCE) and 8 dimension tables that efficiently represent the marketing data hierarchy and characteristics. Here's a detailed explanation of the model design choices:
![image](https://github.com/user-attachments/assets/261e2803-4632-4364-8c82-ef0f5ddf4a0d)

## Rationale for Modeling Decisions

**1.Star Schema for Analytical Efficiency:**

- Chose a star schema over a snowflake schema for simplicity and query performance
- Denormalized certain hierarchical relationships to optimize for analytical queries
- The central fact table (FACT_AD_PERFORMANCE) connects to all relevant dimensions


**2.Handling the Hierarchical Structure:**

- Implemented separate dimension tables for each level of the hierarchy (Account → Sub Account → Portfolio → Campaign → Ad Group → Ad)
- Used foreign keys in the fact table to maintain the relationships between hierarchical levels
- This approach allows for analysis at any level of granularity while maintaining data integrity


**3.Handling Temporal Data and Change Management:**

- Used SCD  Type 2 approach for dimensions with valid_from and valid_to date fields
- This preserves historical values when attributes change (especially for bid and label fields that can change hourly)
- Allows for point-in-time accurate reporting and historical analysis


**4.Platform Independence:**

- DIM_PLATFORM table allows for platform-specific attributes while maintaining a consistent schema
- This approach accommodates different advertising platforms (Google Ads, Facebook, etc.) in a uniform way



1. **Hierarchical Structure Preservation**: By creating separate dimension tables for each level, you've maintained the natural hierarchy of advertising data while enabling analysis at any level of granularity.

2. **Type 2 SCD Implementation**: The valid_from and valid_to fields in all dimension tables support tracking historical changes, which is essential for the frequently changing bid and label attributes.

3. **Platform-Agnostic Design**: DIM_PLATFORM table approach allows for consistent analysis across different advertising platforms despite their variations, creating a unified view of marketing performance.


4. **Hierarchical Data Structure**: The model maintains relationships between accounts, sub-accounts, portfolios, campaigns, ad groups, and ads through well-defined foreign key relationships.

## Trade-offs Considered and Your Chosen Approach

Your model reflects several intentional trade-offs:

1. **Star vs. Snowflake Schema**: You chose a star schema for its query performance benefits despite potential data redundancy. This prioritizes analysis speed over storage efficiency, which is appropriate for an analytics-focused marketing data warehouse.

2. **SCD Type 2 vs. Other SCD Types**: 
   - You implemented SCD Type 2 for all dimensions, which provides complete historical accuracy but increases storage requirements and complexity.
   - This is particularly justified for bid and label fields that change frequently and require historical tracking.

3. **Denormalization vs. Normalization**: 
   - Your approach includes some denormalization (like portfolio information in DIM_CAMPAIGN) to simplify common queries.
   - This trades some data redundancy for improved query performance and usability.

## How Model Handles Data Quality Issues

Your model addresses data quality concerns through:

1. **NULL Handling**: 
   - The model appropriately allows NULLs where specified in the requirements (click_date, view_date, and label).
   - Non-NULL constraints on critical fields ensure data integrity.

2. **Inconsistency Management**: 
   - Using surrogate keys (like account_key, campaign_key) creates a consistent joining mechanism regardless of source platform variations.
   - The platform-specific details are isolated in the DIM_PLATFORM table, allowing the core model to remain consistent.

3. **Data Validation**: The foreign key constraints between fact and dimension tables enforce referential integrity, preventing orphaned performance records.

## Indexing Strategy


1. **Composite Indexes for Common Query Patterns**:
   - Add composite indexes on (platform_key, full_date) for platform-specific time-series analysis
   - Create indexes on (account_key, campaign_key) for account-level campaign performance reporting

2. **Partitioning Strategy**:
   - Partition the FACT_AD_PERFORMANCE table by date (monthly partitions) to improve query performance for time-based analysis
   - platform-based partitioning for very large datasets

3. **Covering Indexes**:
   - Create covering indexes that include commonly queried metric columns (impressions, clicks, cost) alongside dimension keys for reporting queries

4. **Materialized Views**:
   - Implement materialized views for common aggregation patterns (daily/weekly/monthly summaries by campaign, platform, etc.)
   - Refresh these views on a scheduled basis to support dashboard reporting without repetitive complex queries
