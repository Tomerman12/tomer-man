# Data Model Design Rationale
The dimensional model presented in the ERD follows a star schema design with a central fact table (FACT_AD_PERFORMANCE) and 8 dimension tables that efficiently represent the marketing data hierarchy and characteristics. Here's a detailed explanation of the model design choices:
![image](https://github.com/user-attachments/assets/261e2803-4632-4364-8c82-ef0f5ddf4a0d)

## Rationale for Modeling Decisions

**1.Star Schema for Analytical Efficiency:**

- Chose a star schema over a snowflake schema for simplicity and query performance
- Denormalized certain hierarchical relationships to optimize for analytical queries
- The central fact table (FACT_AD_PERFORMANCE) connects to all relevant dimensions


**Handling the Hierarchical Structure:**

- Implemented separate dimension tables for each level of the hierarchy (Account → Sub Account → Portfolio → Campaign → Ad Group → Ad)
- Used foreign keys in the fact table to maintain the relationships between hierarchical levels
- This approach allows for analysis at any level of granularity while maintaining data integrity


**Handling Temporal Data and Change Management:**

- Used SCD  Type 2 approach for dimensions with valid_from and valid_to date fields
- This preserves historical values when attributes change (especially for bid and label fields that can change hourly)
- Allows for point-in-time accurate reporting and historical analysis


**Platform Independence:**

- DIM_PLATFORM table allows for platform-specific attributes while maintaining a consistent schema
- This approach accommodates different advertising platforms (Google Ads, Facebook, etc.) in a uniform way
