#!/bin/bash

# dbt E-Commerce Data Warehouse - Complete Setup Script
# This script creates the entire dbt project structure with all files

set -e

echo "=========================================="
echo "Creating dbt E-Commerce Project"
echo "=========================================="
echo ""

PROJECT_DIR="dbt_ecommerce"

# Create directory structure
echo "Creating directory structure..."
mkdir -p $PROJECT_DIR/{models/{staging,marts/{core,finance}},tests,macros}

# Create dbt_project.yml
echo "Creating dbt_project.yml..."
cat > $PROJECT_DIR/dbt_project.yml << 'EOF'
name: 'ecommerce_dw'
version: '1.0.0'
config-version: 2

profile: 'ecommerce_dw'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  ecommerce_dw:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
      core:
        +materialized: table
      finance:
        +materialized: table
EOF

# Create profiles.yml
echo "Creating profiles.yml..."
cat > $PROJECT_DIR/profiles.yml << 'EOF'
ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: airbyte
      password: airbyte
      dbname: ecommerce_dw
      schema: public
      threads: 4
      keepalives_idle: 0
      connect_timeout: 10
EOF

# Create sources.yml
echo "Creating sources.yml..."
cat > $PROJECT_DIR/models/staging/sources.yml << 'EOF'
version: 2

sources:
  - name: raw
    description: Raw data extracted from APIs
    database: ecommerce_dw
    schema: public
    tables:
      - name: raw_products
        description: Product catalog from DummyJSON
        columns:
          - name: id
            description: Unique product identifier
            tests:
              - unique
              - not_null
              
      - name: raw_users
        description: Customer information from DummyJSON
        columns:
          - name: id
            description: Unique user identifier
            tests:
              - unique
              - not_null
          - name: email
            description: User email address
            tests:
              - unique
              - not_null
              
      - name: raw_orders
        description: Order transactions derived from carts
        columns:
          - name: order_id
            description: Unique order identifier
            tests:
              - unique
              - not_null
          - name: user_id
            description: Customer who placed the order
            tests:
              - not_null
              
      - name: raw_order_items
        description: Line items for each order
        columns:
          - name: order_id
            description: Reference to order
            tests:
              - not_null
          - name: product_id
            description: Reference to product
            tests:
              - not_null
EOF

# Create stg_products.sql
echo "Creating stg_products.sql..."
cat > $PROJECT_DIR/models/staging/stg_products.sql << 'EOF'
{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_products') }}
),

cleaned as (
    select
        id as product_id,
        title as product_name,
        description as product_description,
        category as product_category,
        brand as product_brand,
        sku,
        price::numeric(10,2) as unit_price,
        discount_percentage::numeric(5,2) as discount_percentage,
        round(price * (1 - discount_percentage/100), 2)::numeric(10,2) as discounted_price,
        stock as stock_quantity,
        case 
            when stock = 0 then 'Out of Stock'
            when stock < 10 then 'Low Stock'
            when stock < 50 then 'In Stock'
            else 'Well Stocked'
        end as stock_status,
        rating::numeric(3,2) as rating,
        case
            when rating >= 4.5 then 'Excellent'
            when rating >= 4.0 then 'Good'
            when rating >= 3.0 then 'Average'
            else 'Poor'
        end as rating_category,
        weight::numeric(10,2) as weight,
        thumbnail as thumbnail_url,
        images as image_urls,
        extracted_at as loaded_at,
        current_timestamp as transformed_at
    from source
)

select * from cleaned
EOF

# Create stg_users.sql
echo "Creating stg_users.sql..."
cat > $PROJECT_DIR/models/staging/stg_users.sql << 'EOF'
{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_users') }}
),

cleaned as (
    select
        id as user_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        phone,
        age,
        gender,
        birth_date::date as birth_date,
        (address->>'city')::text as city,
        (address->>'state')::text as state,
        (address->>'country')::text as country,
        (company->>'name')::text as company_name,
        (company->>'title')::text as job_title,
        university,
        extracted_at as loaded_at,
        current_timestamp as transformed_at
    from source
)

select * from cleaned
EOF

# Create stg_orders.sql
echo "Creating stg_orders.sql..."
cat > $PROJECT_DIR/models/staging/stg_orders.sql << 'EOF'
{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

cleaned as (
    select
        order_id,
        user_id,
        order_date::timestamp as order_date,
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        total_amount::numeric(10,2) as order_total,
        discounted_amount::numeric(10,2) as order_total_after_discount,
        (total_amount - discounted_amount)::numeric(10,2) as total_discount_amount,
        total_items as total_quantity,
        status as order_status,
        case when status = 'completed' then true else false end as is_completed,
        case when status = 'cancelled' then true else false end as is_cancelled,
        extracted_at as loaded_at,
        current_timestamp as transformed_at
    from source
)

select * from cleaned
EOF

# Create stg_order_items.sql
echo "Creating stg_order_items.sql..."
cat > $PROJECT_DIR/models/staging/stg_order_items.sql << 'EOF'
{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'raw_order_items') }}
),

cleaned as (
    select
        order_id,
        product_id,
        product_title,
        quantity,
        unit_price::numeric(10,2) as unit_price,
        total_price::numeric(10,2) as line_total,
        discount_percentage::numeric(5,2) as discount_percentage,
        round(unit_price * quantity, 2)::numeric(10,2) as subtotal_before_discount,
        round(unit_price * quantity * (1 - discount_percentage/100), 2)::numeric(10,2) as subtotal_after_discount,
        round(unit_price * quantity * discount_percentage/100, 2)::numeric(10,2) as discount_amount
    from source
)

select * from cleaned
EOF

# Create dim_products.sql
echo "Creating dim_products.sql..."
cat > $PROJECT_DIR/models/marts/core/dim_products.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

product_metrics as (
    select
        product_id,
        count(distinct order_id) as times_ordered,
        sum(quantity) as total_quantity_sold,
        sum(line_total) as total_revenue
    from {{ ref('stg_order_items') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.product_category,
        p.product_brand,
        p.unit_price,
        p.stock_quantity,
        p.rating,
        coalesce(m.times_ordered, 0) as times_ordered,
        coalesce(m.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(m.total_revenue, 0) as total_revenue,
        case
            when m.total_quantity_sold > 20 then 'Bestseller'
            when m.total_quantity_sold > 10 then 'Popular'
            when m.total_quantity_sold > 0 then 'Regular'
            else 'No Sales'
        end as sales_category
    from products p
    left join product_metrics m on p.product_id = m.product_id
)

select * from final
EOF

# Create dim_customers.sql
echo "Creating dim_customers.sql..."
cat > $PROJECT_DIR/models/marts/core/dim_customers.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with users as (
    select * from {{ ref('stg_users') }}
),

customer_metrics as (
    select
        user_id,
        count(distinct order_id) as total_orders,
        sum(case when is_completed then order_total_after_discount else 0 end) as total_spent,
        max(order_date) as last_order_date
    from {{ ref('stg_orders') }}
    group by user_id
),

final as (
    select
        u.user_id,
        u.full_name,
        u.email,
        u.age,
        u.gender,
        u.city,
        u.state,
        u.country,
        coalesce(m.total_orders, 0) as total_orders,
        coalesce(m.total_spent, 0) as lifetime_value,
        m.last_order_date,
        case
            when m.total_orders = 0 then 'Never Ordered'
            when m.total_orders = 1 then 'One-Time'
            when m.total_orders <= 3 then 'Occasional'
            else 'Frequent'
        end as customer_segment
    from users u
    left join customer_metrics m on u.user_id = m.user_id
)

select * from final
EOF

# Create fact_orders.sql
echo "Creating fact_orders.sql..."
cat > $PROJECT_DIR/models/marts/core/fact_orders.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        order_id,
        user_id,
        order_date,
        order_year,
        order_month,
        order_total,
        order_total_after_discount,
        total_discount_amount,
        total_quantity,
        order_status,
        is_completed,
        is_cancelled,
        case
            when order_total_after_discount < 50 then 'Small'
            when order_total_after_discount < 150 then 'Medium'
            when order_total_after_discount < 300 then 'Large'
            else 'Extra Large'
        end as order_size
    from orders
)

select * from final
EOF

# Create fact_order_items.sql
echo "Creating fact_order_items.sql..."
cat > $PROJECT_DIR/models/marts/core/fact_order_items.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, user_id, order_date, order_status, is_completed
    from {{ ref('stg_orders') }}
),

final as (
    select
        i.order_id,
        i.product_id,
        o.user_id,
        o.order_date,
        o.order_status,
        i.quantity,
        i.unit_price,
        i.line_total,
        i.discount_amount
    from order_items i
    inner join orders o on i.order_id = o.order_id
)

select * from final
EOF

# Create daily_revenue.sql
echo "Creating daily_revenue.sql..."
cat > $PROJECT_DIR/models/marts/finance/daily_revenue.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with daily_orders as (
    select
        date(order_date) as order_date,
        count(distinct order_id) as total_orders,
        count(distinct user_id) as unique_customers,
        sum(case when is_completed then order_total_after_discount else 0 end) as total_revenue,
        avg(case when is_completed then order_total_after_discount end) as avg_order_value
    from {{ ref('fact_orders') }}
    group by date(order_date)
)

select * from daily_orders
order by order_date desc
EOF

# Create README.md
echo "Creating README.md..."
cat > $PROJECT_DIR/README.md << 'EOF'
# dbt E-Commerce Data Warehouse

## Quick Start

1. Install dbt:
```bash
pip install dbt-postgres
```

2. Configure connection:
```bash
cp profiles.yml ~/.dbt/profiles.yml
```

3. Test connection:
```bash
dbt debug
```

4. Run models:
```bash
dbt run
```

5. Run tests:
```bash
dbt test
```

6. Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

## Models

### Staging (Views)
- `stg_products` - Clean product data
- `stg_users` - Clean customer data
- `stg_orders` - Clean order data
- `stg_order_items` - Clean line item data

### Core Marts (Tables)
- `dim_products` - Product dimension with sales metrics
- `dim_customers` - Customer dimension with lifetime value
- `fact_orders` - Order-level transactions
- `fact_order_items` - Line-level detail

### Finance Marts (Tables)
- `daily_revenue` - Daily KPIs and metrics

## Workflow

1. Run Airflow DAG to extract raw data
2. Run `dbt run` to transform data
3. Run `dbt test` to validate
4. Query marts in Metabase or SQL client
EOF

echo ""
echo "=========================================="
echo "✓ dbt Project Created Successfully!"
echo "=========================================="
echo ""
echo "Project location: ./$PROJECT_DIR"
echo ""
echo "Next steps:"
echo "1. cd $PROJECT_DIR"
echo "2. pip install dbt-postgres"
echo "3. cp profiles.yml ~/.dbt/profiles.yml"
echo "4. dbt debug"
echo "5. dbt run"
echo ""
