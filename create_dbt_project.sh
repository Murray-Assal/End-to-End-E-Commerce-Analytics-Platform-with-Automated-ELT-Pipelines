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
mkdir -p $PROJECT_DIR/{models/{staging,intermediate,marts/{core,finance}},tests,macros}

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
    intermediate:
      +materialized: view
      +schema: intermediate
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
      user: admin
      password: admin
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
        username,
        age,
        gender,
        birth_date::date as birth_date,
        (address->>'street')::text as street_address,
        (address->>'postalCode')::text as postal_code,
        (address->>'city')::text as city,
        (address->>'state')::text as state,
        (address->>'stateCode')::text as state_code,
        (address->>'country')::text as country,
        (company->>'name')::text as company_name,
        (company->>'department')::text as department,
        (company->>'title')::text as job_title,
        university,
        blood_group,
        height as height_cm,
        weight as weight_kg,
        eye_color,
        hair_color,
        hair_type,
        (bank->>'cardType')::text as card_type,
        (bank->>'currency')::text as currency,
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

#create int_users_cleaned_locations.sql
echo "Creating int_users_cleaned_locations.sql..."
cat > $PROJECT_DIR/models/intermediate/int_users_cleaned_locations.sql << 'EOF'
{{
    config(
        materialized='view'
    )
}}

with users as (
    select * from {{ ref('stg_users') }}
),

-- City to State mapping for common US cities
city_state_reference as (
    select 
        city,
        correct_state,
        correct_state_code
    from (
        values
            -- Arizona
            ('Phoenix', 'Arizona', 'AZ'),
            ('Tucson', 'Arizona', 'AZ'),
            ('Mesa', 'Arizona', 'AZ'),
            -- California
            ('Los Angeles', 'California', 'CA'),
            ('San Diego', 'California', 'CA'),
            ('San Jose', 'California', 'CA'),
            ('San Francisco', 'California', 'CA'),
            ('Sacramento', 'California', 'CA'),
            ('Oakland', 'California', 'CA'),
            -- Colorado
            ('Denver', 'Colorado', 'CO'),
            ('Colorado Springs', 'Colorado', 'CO'),
            -- Florida
            ('Jacksonville', 'Florida', 'FL'),
            ('Miami', 'Florida', 'FL'),
            ('Tampa', 'Florida', 'FL'),
            ('Orlando', 'Florida', 'FL'),
            -- Georgia
            ('Atlanta', 'Georgia', 'GA'),
            ('Savannah', 'Georgia', 'GA'),
            -- Illinois
            ('Chicago', 'Illinois', 'IL'),
            ('Naperville', 'Illinois', 'IL'),
            -- Indiana
            ('Indianapolis', 'Indiana', 'IN'),
            ('Fort Wayne', 'Indiana', 'IN'),
            -- Massachusetts
            ('Boston', 'Massachusetts', 'MA'),
            ('Worcester', 'Massachusetts', 'MA'),
            -- Michigan
            ('Detroit', 'Michigan', 'MI'),
            ('Grand Rapids', 'Michigan', 'MI'),
            -- Minnesota
            ('Minneapolis', 'Minnesota', 'MN'),
            ('St. Paul', 'Minnesota', 'MN'),
            -- Missouri
            ('Kansas City', 'Missouri', 'MO'),
            ('St. Louis', 'Missouri', 'MO'),
            -- Nevada
            ('Las Vegas', 'Nevada', 'NV'),
            ('Reno', 'Nevada', 'NV'),
            -- New York
            ('New York', 'New York', 'NY'),
            ('Buffalo', 'New York', 'NY'),
            ('Rochester', 'New York', 'NY'),
            -- North Carolina
            ('Charlotte', 'North Carolina', 'NC'),
            ('Raleigh', 'North Carolina', 'NC'),
            -- Ohio
            ('Cleveland', 'Ohio', 'OH'),
            ('Cincinnati', 'Ohio', 'OH'),
            ('Columbus', 'Ohio', 'OH'),
            -- Oklahoma
            ('Oklahoma City', 'Oklahoma', 'OK'),
            ('Tulsa', 'Oklahoma', 'OK'),
            -- Oregon
            ('Portland', 'Oregon', 'OR'),
            ('Eugene', 'Oregon', 'OR'),
            -- Pennsylvania
            ('Philadelphia', 'Pennsylvania', 'PA'),
            ('Pittsburgh', 'Pennsylvania', 'PA'),
            -- Tennessee
            ('Nashville', 'Tennessee', 'TN'),
            ('Memphis', 'Tennessee', 'TN'),
            -- Texas
            ('Austin', 'Texas', 'TX'),
            ('Dallas', 'Texas', 'TX'),
            ('Houston', 'Texas', 'TX'),
            ('San Antonio', 'Texas', 'TX'),
            ('Fort Worth', 'Texas', 'TX'),
            ('El Paso', 'Texas', 'TX'),
            -- Utah
            ('Salt Lake City', 'Utah', 'UT'),
            -- Virginia
            ('Virginia Beach', 'Virginia', 'VA'),
            ('Norfolk', 'Virginia', 'VA'),
            ('Richmond', 'Virginia', 'VA'),
            -- Washington
            ('Seattle', 'Washington', 'WA'),
            ('Spokane', 'Washington', 'WA'),
            ('Tacoma', 'Washington', 'WA'),
            -- Wisconsin
            ('Milwaukee', 'Wisconsin', 'WI'),
            ('Madison', 'Wisconsin', 'WI')
    ) as t(city, correct_state, correct_state_code)
),

cleaned_locations as (
    select
        u.user_id,
        u.city,
        u.state as original_state,
        u.state_code as original_state_code,
        
        -- Use reference table to get correct state if city is known
        coalesce(r.correct_state, u.state) as cleaned_state,
        coalesce(r.correct_state_code, u.state_code) as cleaned_state_code,
        
        -- Flag if we corrected it
        case 
            when r.correct_state is not null and r.correct_state != u.state then true
            else false
        end as was_corrected
        
    from users u
    left join city_state_reference r on u.city = r.city
)

select 
    user_id,
    city,
    cleaned_state as state,
    cleaned_state_code as state_code,
    city || ', ' || cleaned_state_code as city_state,
    original_state,
    original_state_code,
    was_corrected
from cleaned_locations
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
        -- Keys
        p.product_id,
        
        -- Product attributes
        p.product_name,
        p.product_category,
        p.product_brand,
        p.sku,
        
        -- Pricing
        p.unit_price,
        p.discount_percentage,
        p.discounted_price,
        
        -- Inventory
        p.stock_quantity,
        p.stock_status,
        
        -- Quality metrics
        p.rating,
        p.rating_category,
        
        -- Physical
        p.weight,
        
        -- Media
        p.thumbnail_url,
        p.image_urls,
        
        -- Sales metrics (from order items)
        coalesce(m.times_ordered, 0) as times_ordered,
        coalesce(m.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(m.total_revenue, 0) as total_revenue,
        
        -- Calculated metrics
        case 
            when m.times_ordered > 0 
            then round(m.total_revenue / m.total_quantity_sold, 2)
            else p.unit_price 
        end as avg_selling_price,
        
        case
            when m.total_quantity_sold > 20 then 'Bestseller'
            when m.total_quantity_sold > 10 then 'Popular'
            when m.total_quantity_sold > 0 then 'Regular'
            else 'No Sales'
        end as sales_category,
        
        -- Metadata
        p.loaded_at,
        p.transformed_at
        
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

cleaned_locations as (
    select * from {{ ref('int_users_cleaned_locations') }}
),

customer_metrics as (
    select
        user_id,
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        sum(case when is_completed then order_total_after_discount else 0 end) as total_spent,
        avg(case when is_completed then order_total_after_discount end) as avg_order_value,
        max(order_date) as last_order_date,
        min(order_date) as first_order_date
    from {{ ref('stg_orders') }}
    group by user_id
),

final as (
    select
        -- Keys
        u.user_id,
        
        -- Personal info
        u.first_name,
        u.last_name,
        u.full_name,
        u.email,
        u.phone,
        u.username,
        
        -- Demographics
        u.age,
        case
            when u.age < 25 then '18-24'
            when u.age < 35 then '25-34'
            when u.age < 45 then '35-44'
            when u.age < 55 then '45-54'
            else '55+'
        end as age_group,
        u.gender,
        u.birth_date,
        
        -- Location (CLEANED - using the corrected city/state data)
        u.street_address,
        l.city,
        l.state,              -- Corrected state
        l.state_code,         -- Corrected state code
        u.postal_code,
        u.country,
        l.city_state,         -- Formatted: "City, ST"
        
        -- Professional
        u.company_name,
        u.job_title,
        u.department,
        u.university,
        
        -- Health metrics
        u.blood_group,
        u.height_cm,
        u.weight_kg,
        
        -- Physical appearance
        u.eye_color,
        u.hair_color,
        u.hair_type,
        
        -- Payment info
        u.card_type,
        u.currency,
        
        -- Customer metrics (from orders)
        coalesce(m.total_orders, 0) as total_orders,
        coalesce(m.completed_orders, 0) as completed_orders,
        coalesce(m.total_spent, 0) as lifetime_value,
        coalesce(m.avg_order_value, 0) as avg_order_value,
        m.first_order_date,
        m.last_order_date,
        
        -- Customer segmentation
        case
            when m.total_orders = 0 then 'Never Ordered'
            when m.total_orders = 1 then 'One-Time'
            when m.total_orders <= 3 then 'Occasional'
            when m.total_orders <= 5 then 'Regular'
            else 'Frequent'
        end as customer_segment,
        
        case
            when m.total_spent >= 1000 then 'VIP'
            when m.total_spent >= 500 then 'Premium'
            when m.total_spent >= 200 then 'Standard'
            when m.total_spent > 0 then 'Basic'
            else 'No Purchase'
        end as value_segment,
        
        -- Recency
        case
            when m.last_order_date is null then 'Never Ordered'
            when m.last_order_date >= current_date - interval '7 days' then 'Active'
            when m.last_order_date >= current_date - interval '30 days' then 'Recent'
            when m.last_order_date >= current_date - interval '90 days' then 'Lapsed'
            else 'Dormant'
        end as recency_segment,
        
        -- Metadata
        u.loaded_at,
        u.transformed_at
        
    from users u
    left join cleaned_locations l on u.user_id = l.user_id
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

### Intermediate (Views)
- `int_users_cleaned_locations` - Users with cleaned location data

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
