# E-Commerce Data Platform

A production-ready, end-to-end data engineering pipeline built with modern data stack tools. This project demonstrates the complete data lifecycle from extraction to visualization, showcasing best practices in data engineering, ETL/ELT workflows, and analytics.

![Data Engineering](https://img.shields.io/badge/Data-Engineering-blue)
![Python](https://img.shields.io/badge/Python-3.12-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Airflow](https://img.shields.io/badge/Apache-Airflow_3.1.1-red)
![dbt](https://img.shields.io/badge/dbt-1.7-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Metabase](https://img.shields.io/badge/Metabase-Latest-purple)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Data Model](#data-model)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Dashboards](#dashboards)
- [Data Quality](#data-quality)
- [Lessons Learned](#lessons-learned)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project implements a **modern data warehouse** for an e-commerce business, providing actionable insights through automated data pipelines and interactive dashboards. The platform processes data from multiple sources, transforms it using industry-standard tools, and serves it to business users through intuitive visualizations.

**Key Capabilities:**

- **Real-time Analytics**: Daily automated data refreshes
- **Scalable ETL**: Modular, maintainable data pipelines
- **Business Intelligence**: 5 comprehensive dashboards covering sales, customers, and products
- **Data Quality**: Built-in validation and cleaning layers
- **Containerized**: Fully dockerized for easy deployment

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│  ┌──────────────────┐              ┌─────────────────────┐          │
│  │  DummyJSON API   │              │    Stripe API       │          │
│  │  • Products      │              │    • Payments       │          │
│  │  • Users         │              │    • Refunds        │          │
│  │  • Carts         │              │    • Invoices       │          │
│  └──────────────────┘              └─────────────────────┘          │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Apache Airflow)                  │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  DAG: ecommerce_data_extraction                                │ │
│  │  • Schedule: @daily                                            │ │
│  │  • Extracts raw data via REST APIs                             │ │
│  │  • Parallel execution for independent sources                  │ │
│  │  • Error handling & retry logic                                │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA WAREHOUSE (PostgreSQL)                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  RAW LAYER                                                   │   │
│  │  • raw_products, raw_users, raw_carts                        │   │
│  │  • raw_orders, raw_order_items                               │   │
│  │  • raw_stripe_payments, raw_stripe_refunds                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION (dbt)                             │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  STAGING LAYER (Views)                                       │   │
│  │  • stg_products, stg_users, stg_orders                       │   │
│  │  • Data cleaning, type casting, JSONB extraction             │   │
│  └──────────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  INTERMEDIATE LAYER (Views)                                  │   │
│  │  • int_users_cleaned_locations (Data quality fixes)          │   │
│  │  • Business logic, data enrichment                           │   │
│  └──────────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  MARTS LAYER (Tables)                                        │   │
│  │  Core:                                                       │   │
│  │    • dim_products, dim_customers                             │   │
│  │    • fact_orders, fact_order_items                           │   │
│  │  Finance:                                                    │   │
│  │    • daily_revenue, product_performance                      │   │
│  │    • customer_segmentation                                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    VISUALIZATION (Metabase)                         │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  • Executive Overview Dashboard                              │   │
│  │  • Sales Performance Dashboard                               │   │
│  │  • Customer Analytics Dashboard                              │   │
│  │  • Product Performance Dashboard                             │   │
│  │  • Operations & KPIs Dashboard                               │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

### Orchestration

- **Apache Airflow 3.1.1**: Workflow orchestration with TaskFlow API
- **Docker Compose**: Container orchestration

### Storage

- **PostgreSQL 16**: Data warehouse
- **Separate databases**: Airflow metadata, Metabase metadata, data warehouse

### Transformation

- **dbt (Data Build Tool)**: SQL-based transformations
- **Python 3.12**: Custom data processing scripts

### Visualization

- **Metabase**: Self-service BI platform

### Data Sources

- **DummyJSON API**: Mock e-commerce data (products, users, carts)
- **Stripe API**: Payment processing data (optional)

## Features

### Data Pipeline

- **Automated Daily Extraction**: Airflow DAG runs daily at midnight
- **Parallel Processing**: Independent API calls run concurrently
- **Error Handling**: Retry logic with exponential backoff
- **Idempotent Loads**: TRUNCATE/INSERT pattern ensures data consistency
- **Incremental Ready**: Architecture supports incremental loads

### Data Transformation

- **Layered Architecture**: Raw → Staging → Intermediate → Marts
- **Data Quality**: City/state validation with reference mapping
- **Type Safety**: Proper data type casting and validation
- **Documentation**: Auto-generated dbt docs
- **Testing**: Built-in data quality tests (uniqueness, not null)

### Analytics

- **Star Schema**: Optimized for analytical queries
- **Pre-aggregated Metrics**: Daily summaries for performance
- **Customer Segmentation**: RFM analysis for customer insights
- **Product Analytics**: Sales categories, inventory health
- **Financial Reporting**: Revenue trends, order metrics

## Data Model

### Dimensional Model (Star Schema)

```
                    ┌─────────────────────┐
                    │   dim_customers     │
                    ├─────────────────────┤
                    │ PK: user_id         │
                    │ • full_name         │
                    │ • email             │
                    │ • city_state        │
                    │ • age_group         │
                    │ • lifetime_value    │
                    │ • customer_segment  │
                    └─────────────────────┘
                             │
                             │
                             ↓
┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
│   dim_products      │  │   fact_orders        │  │   fact_order_items  │
├─────────────────────┤  ├──────────────────────┤  ├─────────────────────┤
│ PK: product_id      │←─│ FK: product_id       │  │ FK: order_id        │
│ • product_name      │  │ FK: user_id          │  │ FK: product_id      │
│ • category          │  │ PK: order_id         │─→│ FK: user_id         │
│ • unit_price        │  │ • order_date         │  │ • quantity          │
│ • rating            │  │ • order_total        │  │ • line_total        │
│ • stock_quantity    │  │ • order_status       │  │ • discount_amount   │
│ • sales_category    │  │ • is_completed       │  └─────────────────────┘
└─────────────────────┘  └──────────────────────┘
```

### Metrics & Aggregations

**daily_revenue**

- Daily KPIs: revenue, orders, customers
- Average order value trends
- Order completion rates

**product_performance**

- Sales rankings by revenue/quantity
- Inventory health indicators
- Category performance metrics

**customer_segmentation**

- RFM analysis (Recency, Frequency, Monetary)
- Customer lifecycle stages
- Value-based segmentation

## Getting Started

### Prerequisites

- Docker & Docker Compose
- 4GB+ RAM
- 10GB+ disk space
- Python 3.12+ (for local dbt development)

### Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/Murray-Assal/End-to-End-E-Commerce-Analytics-Platform-with-Automated-ELT-Pipelines.git
   cd ecommerce-data-platform
   ```

2. **Set up environment variables**

   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

3. **Start the platform**

   ```bash
   docker-compose up -d
   ```

4. **Wait for services to initialize** (~2 minutes)

   ```bash
   docker-compose logs -f
   ```

5. **Access the services**

   - **Airflow**: <http://localhost:8080> (user: `airflow`, password: `airflow`)
   - **Metabase**: <http://localhost:3000>
   - **PostgreSQL**: `localhost:5433` (user: `admin`, password: `admin`)

### Initial Setup

1. **Trigger the extraction DAG**
   - Go to Airflow UI
   - Toggle on `ecommerce_data_extraction`
   - Click "Trigger DAG"

2. **Run dbt transformations**

   ```bash
   cd dbt_ecommerce
   pip install dbt-postgres
   cp profiles.yml ~/.dbt/profiles.yml
   dbt run
   ```

3. **Set up Metabase**
   - Go to <http://localhost:3000>
   - Create admin account
   - Connect to database:
     - Host: `postgres-data`
     - Port: `5432`
     - Database: `ecommerce_dw`
     - User: `admin`
     - Password: `admin`

4. **Build dashboards**
   - Use SQL queries from `METABASE_ANALYTICS_GUIDE.md`
   - Create the 5 pre-defined dashboards

## Project Structure

```
ecommerce-data-platform/
├── dags/
│   └── ecommerce_extraction_dag.py    # Airflow DAG for data extraction
├── dbt_ecommerce/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml            # Source definitions & tests
│   │   │   ├── stg_products.sql       # Staging: products
│   │   │   ├── stg_users.sql          # Staging: users
│   │   │   ├── stg_orders.sql         # Staging: orders
│   │   │   └── stg_order_items.sql    # Staging: order items
│   │   ├── intermediate/
│   │   │   └── int_users_cleaned_locations.sql  # Data quality layer
│   │   └── marts/
│   │       ├── core/
│   │       │   ├── dim_products.sql   # Product dimension
│   │       │   ├── dim_customers.sql  # Customer dimension
│   │       │   ├── fact_orders.sql    # Order facts
│   │       │   └── fact_order_items.sql  # Order item facts
│   │       └── finance/
│   │           ├── daily_revenue.sql  # Daily KPIs
│   │           ├── product_performance.sql
│   │           └── customer_segmentation.sql
│   ├── dbt_project.yml               # dbt configuration
│   ├── profiles.yml                  # Database connection
│   └── README.md                     # dbt documentation
├── docs/
│   ├── METABASE_SETUP_GUIDE.md      # Metabase setup instructions
│   ├── METABASE_ANALYTICS_GUIDE.md  # Dashboard queries & tips
│   └── HOW_TO_VIEW_DATA.md          # Database access guide
├── docker-compose.yml                # Container orchestration
├── .env                              # Environment variables
└── README.md                         # This file
```

## Dashboards

### 1. Executive Overview

**Purpose**: High-level KPIs for leadership

**Metrics**:

- Total Revenue, Total Orders, Average Order Value, Total Customers
- Revenue trend line chart
- Orders by status (pie chart)
- Top 5 products by revenue
- Customer segment distribution

### 2. Sales Performance

**Purpose**: Deep dive into sales metrics

**Metrics**:

- Revenue by product category
- Daily revenue trends
- Order size distribution
- Top 20 products table
- Discount effectiveness analysis

### 3. Customer Analytics

**Purpose**: Understand customer behavior

**Metrics**:

- Customer lifetime value distribution
- Top 10 customers by spend
- Demographics (age groups, gender, location)
- Customer segments (one-time, occasional, frequent)
- Repeat vs one-time customer ratio

### 4. Product Performance

**Purpose**: Analyze product catalog

**Metrics**:

- Products by category with ratings
- Best sellers table
- Low stock alerts
- Rating distribution
- Sales categories (bestseller, popular, regular)

### 5. Operations & KPIs

**Purpose**: Daily operations monitoring

**Metrics**:

- Today's performance (revenue, orders, customers, AOV)
- 7-day trend chart
- Order completion rate
- Recent orders table
- Average items per order

## Data Quality

### Built-in Validation

**Source Layer Tests** (in `sources.yml`):

- Primary key uniqueness
- Not null constraints on critical fields
- Referential integrity between tables

**Data Cleaning**:

- **City/State Correction**: Reference mapping for 50+ major US cities
- **Type Casting**: Proper numeric, date, and boolean types
- **JSONB Extraction**: Clean extraction from nested JSON structures
- **NULL Handling**: Coalesce patterns for missing values

**Monitoring**:

- Run `dbt test` after each transformation
- Check logs in `dbt_ecommerce/logs/dbt.log`
- Review data quality in Metabase dashboards

## Lessons Learned

### Technical Insights

1. **Airbyte Complexity**: Initially attempted Airbyte but pivoted to Python scripts due to:
   - Docker Compose deprecation in Airbyte 1.0+
   - Resource constraints (abctl requires 50-100GB)
   - Simpler architecture with direct Python → PostgreSQL

2. **dbt Schema Configuration**: Learned about schema naming patterns:
   - `schema: public` + custom schema = `public_staging`, `public_marts`
   - Better to use separate schemas or configure custom naming

3. **Test Data Limitations**: DummyJSON has inherent data quality issues:
   - Implemented data quality layer to fix city/state mismatches
   - Good practice for real-world scenarios

4. **Container Orchestration**: Separation of concerns:
   - Airflow metadata → `postgres` database
   - Metabase metadata → `metabase` database  
   - Data warehouse → `postgres-data` database
   - Prevents conflicts and enables independent scaling

### Best Practices Applied

- **Idempotent Pipelines**: TRUNCATE/INSERT pattern for full refreshes
- **Separation of Concerns**: Modular dbt models (staging → marts)
- **Documentation as Code**: dbt docs and inline comments
- **Version Control**: All code in Git with meaningful commits
- **Testing**: Data quality tests at every layer
- **Monitoring**: Airflow task logs and dbt test results

## Future Enhancements

### Short Term

- [ ] Add incremental loading for large tables
- [ ] Implement SCD Type 2 for dim_customers
- [ ] Add data quality monitoring dashboard
- [ ] Set up email alerts for pipeline failures
- [ ] Add more dbt tests (relationships, accepted values)

### Medium Term

- [ ] Migrate to cloud (AWS/GCP/Azure)
- [ ] Add real-time streaming with Kafka
- [ ] Implement data lineage tracking
- [ ] Add machine learning models (customer churn prediction)
- [ ] Create Airflow sensor for external data availability

### Long Term

- [ ] Multi-tenant architecture
- [ ] Data catalog integration (DataHub/Amundsen)
- [ ] Implement data mesh principles
- [ ] Add cost monitoring and optimization
- [ ] Build self-service analytics platform

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **DummyJSON**: For providing a reliable mock API for testing
- **Apache Airflow**: For robust workflow orchestration
- **dbt Labs**: For the incredible transformation framework
- **Metabase**: For making BI accessible and open-source
- **Data Engineering Community**: For best practices and inspiration

## Contact

Murad Asal - [My LinkedIn](https://www.linkedin.com/in/murad-asal-421ba7226) - <muradsaleh82@gmail.com>

Project Link: [https://github.com/Murray-Assal/End-to-End-E-Commerce-Analytics-Platform-with-Automated-ELT-Pipelines](https://github.com/Murray-Assal/End-to-End-E-Commerce-Analytics-Platform-with-Automated-ELT-Pipelines)

---

**If you found this project helpful, please consider giving it a star!**

---

## Project Statistics

- **Lines of SQL**: ~2,000
- **dbt Models**: 15+
- **Airflow Tasks**: 9
- **Data Tables**: 20+
- **API Endpoints**: 5
- **Dashboards**: 5
- **Metrics Tracked**: 50+

## Skills Demonstrated

- Data Pipeline Development
- ETL/ELT Architecture
- SQL & Data Modeling
- Python Programming
- Docker & Containerization
- Workflow Orchestration (Airflow)
- Data Transformation (dbt)
- Business Intelligence
- Data Quality Engineering
- Database Design
- API Integration
- Version Control (Git)
- Technical Documentation

---

Built with ❤️ by Murad Asal | Last Updated: December 2025
