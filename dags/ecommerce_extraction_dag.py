"""
E-Commerce Data Platform - Airflow DAG
Orchestrates extraction from DummyJSON and Stripe APIs using TaskFlow API
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from datetime import timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import json
import os


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='ecommerce_data_extraction',
    default_args=default_args,
    description='Extract data from DummyJSON and Stripe APIs',
    schedule='@daily',
    start_date=datetime(2025, 12, 15),
    catchup=False,
    tags=['extraction', 'ecommerce', 'api'],
)
def ecommerce_extraction_pipeline():
    """
    Main DAG for extracting e-commerce data from multiple sources
    """
    
    @task()
    def get_db_connection():
        """Get database connection string from environment or default"""
        return os.getenv(
            "DATABASE_URL",
            "postgresql://admin:admin@postgres-data:5432/ecommerce_dw"
        )
    
    @task()
    def extract_products(db_conn: str):
        """Extract products from DummyJSON API"""
        print("Extracting products from DummyJSON...")
        
        # Fetch data from API
        response = requests.get("https://dummyjson.com/products?limit=0")
        response.raise_for_status()
        products = response.json().get('products', [])
        
        print(f"Found {len(products)} products")
        
        # Connect to database
        conn = psycopg2.connect(db_conn)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_products (
                id INTEGER PRIMARY KEY,
                title TEXT,
                description TEXT,
                category TEXT,
                price DECIMAL(10,2),
                discount_percentage DECIMAL(5,2),
                rating DECIMAL(3,2),
                stock INTEGER,
                brand TEXT,
                sku TEXT,
                weight DECIMAL(10,2),
                thumbnail TEXT,
                images JSONB,
                extracted_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Clear and insert data
        cursor.execute("TRUNCATE TABLE raw_products")
        
        product_data = [
            (
                p['id'], p.get('title'), p.get('description'), p.get('category'),
                p.get('price'), p.get('discountPercentage'), p.get('rating'),
                p.get('stock'), p.get('brand'), p.get('sku'), p.get('weight'),
                p.get('thumbnail'), json.dumps(p.get('images', []))
            )
            for p in products
        ]
        
        execute_values(
            cursor,
            """
            INSERT INTO raw_products 
            (id, title, description, category, price, discount_percentage, 
             rating, stock, brand, sku, weight, thumbnail, images)
            VALUES %s
            """,
            product_data
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Inserted {len(products)} products")
        return {'products_count': len(products)}
    
    @task()
    def extract_users(db_conn: str):
        """Extract users from DummyJSON API"""
        print("Extracting users from DummyJSON...")
        
        response = requests.get("https://dummyjson.com/users?limit=0")
        response.raise_for_status()
        users = response.json().get('users', [])
        
        print(f"Found {len(users)} users")
        
        conn = psycopg2.connect(db_conn)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_users (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                maiden_name TEXT,
                age INTEGER,
                gender TEXT,
                email TEXT,
                phone TEXT,
                username TEXT,
                birth_date DATE,
                image TEXT,
                blood_group TEXT,
                height DECIMAL(5,2),
                weight DECIMAL(5,2),
                eye_color TEXT,
                hair_color TEXT,
                hair_type TEXT,
                ip TEXT,
                address JSONB,
                mac_address TEXT,
                university TEXT,
                bank JSONB,
                company JSONB,
                ein TEXT,
                ssn TEXT,
                user_agent TEXT,
                crypto JSONB,
                role TEXT,
                extracted_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cursor.execute("TRUNCATE TABLE raw_users")
        
        user_data = [
            (
                u['id'], u.get('firstName'), u.get('lastName'), u.get('maidenName'),
                u.get('age'), u.get('gender'), u.get('email'), u.get('phone'),
                u.get('username'), u.get('birthDate'), u.get('image'), u.get('bloodGroup'),
                u.get('height'), u.get('weight'), u.get('eyeColor'),
                u.get('hair', {}).get('color'), u.get('hair', {}).get('type'),
                u.get('ip'), json.dumps(u.get('address', {})), u.get('macAddress'),
                u.get('university'), json.dumps(u.get('bank', {})),
                json.dumps(u.get('company', {})), u.get('ein'), u.get('ssn'),
                u.get('userAgent'), json.dumps(u.get('crypto', {})), u.get('role')
            )
            for u in users
        ]
        
        execute_values(
            cursor,
            """
            INSERT INTO raw_users 
            (id, first_name, last_name, maiden_name, age, gender, email, phone, 
             username, birth_date, image, blood_group, height, weight, eye_color,
             hair_color, hair_type, ip, address, mac_address, university, bank,
             company, ein, ssn, user_agent, crypto, role)
            VALUES %s
            """,
            user_data
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Inserted {len(users)} users")
        return {'users_count': len(users)}
    
    @task()
    def extract_carts(db_conn: str):
        """Extract carts from DummyJSON API"""
        print("Extracting carts from DummyJSON...")
        
        response = requests.get("https://dummyjson.com/carts?limit=0")
        response.raise_for_status()
        carts = response.json().get('carts', [])
        
        print(f"Found {len(carts)} carts")
        
        conn = psycopg2.connect(db_conn)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_carts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                total DECIMAL(10,2),
                discounted_total DECIMAL(10,2),
                total_products INTEGER,
                total_quantity INTEGER,
                extracted_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_cart_items (
                cart_id INTEGER,
                product_id INTEGER,
                title TEXT,
                price DECIMAL(10,2),
                quantity INTEGER,
                total DECIMAL(10,2),
                discount_percentage DECIMAL(5,2),
                discounted_total DECIMAL(10,2),
                thumbnail TEXT,
                extracted_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (cart_id, product_id)
            )
        """)
        
        cursor.execute("TRUNCATE TABLE raw_carts CASCADE")
        cursor.execute("TRUNCATE TABLE raw_cart_items")
        
        # Insert carts
        cart_data = [
            (c['id'], c.get('userId'), c.get('total'), c.get('discountedTotal'),
             c.get('totalProducts'), c.get('totalQuantity'))
            for c in carts
        ]
        
        execute_values(
            cursor,
            """
            INSERT INTO raw_carts 
            (id, user_id, total, discounted_total, total_products, total_quantity)
            VALUES %s
            """,
            cart_data
        )
        
        # Insert cart items
        cart_items = []
        for cart in carts:
            for product in cart.get('products', []):
                cart_items.append((
                    cart['id'], product.get('id'), product.get('title'),
                    product.get('price'), product.get('quantity'), product.get('total'),
                    product.get('discountPercentage'), product.get('discountedTotal'),
                    product.get('thumbnail')
                ))
        
        execute_values(
            cursor,
            """
            INSERT INTO raw_cart_items 
            (cart_id, product_id, title, price, quantity, total, 
             discount_percentage, discounted_total, thumbnail)
            VALUES %s
            """,
            cart_items
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Inserted {len(carts)} carts and {len(cart_items)} cart items")
        return {'carts_count': len(carts), 'cart_items_count': len(cart_items)}
    
    @task()
    def derive_orders(db_conn: str, carts_result: dict):
        """Derive orders from carts data"""
        print("Deriving orders from carts...")
        
        conn = psycopg2.connect(db_conn)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                order_date TIMESTAMP,
                total_amount DECIMAL(10,2),
                discounted_amount DECIMAL(10,2),
                total_items INTEGER,
                status TEXT DEFAULT 'completed',
                extracted_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_order_items (
                order_id INTEGER,
                product_id INTEGER,
                product_title TEXT,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_price DECIMAL(10,2),
                discount_percentage DECIMAL(5,2),
                PRIMARY KEY (order_id, product_id)
            )
        """)
        
        cursor.execute("TRUNCATE TABLE raw_orders CASCADE")
        cursor.execute("TRUNCATE TABLE raw_order_items")
        
        # Convert carts to orders with random dates and statuses
        cursor.execute("""
            INSERT INTO raw_orders (order_id, user_id, order_date, total_amount, 
                                   discounted_amount, total_items, status)
            SELECT 
                id as order_id,
                user_id,
                NOW() - (RANDOM() * INTERVAL '30 days') as order_date,
                total as total_amount,
                discounted_total as discounted_amount,
                total_quantity as total_items,
                CASE 
                    WHEN RANDOM() < 0.85 THEN 'completed'
                    WHEN RANDOM() < 0.93 THEN 'shipped'
                    WHEN RANDOM() < 0.97 THEN 'pending'
                    ELSE 'cancelled'
                END as status
            FROM raw_carts
        """)
        
        # Convert cart items to order items
        cursor.execute("""
            INSERT INTO raw_order_items (order_id, product_id, product_title, 
                                        quantity, unit_price, total_price, 
                                        discount_percentage)
            SELECT 
                cart_id as order_id,
                product_id,
                title as product_title,
                quantity,
                price as unit_price,
                total as total_price,
                discount_percentage
            FROM raw_cart_items
        """)
        
        orders_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Derived {orders_count} orders from carts")
        return {'orders_count': orders_count}
    
    @task()
    def extract_stripe_payments(db_conn: str):
        """Extract payments from Stripe API (optional - requires API key)"""
        stripe_key = os.getenv("STRIPE_API_KEY")
        
        if not stripe_key or stripe_key == "sk_test_YOUR_KEY_HERE":
            print("⚠️  No Stripe API key found. Skipping payment extraction.")
            return {'payments_count': 0, 'skipped': True}
        
        print("Extracting payments from Stripe...")
        
        try:
            import stripe
            stripe.api_key = stripe_key
            
            conn = psycopg2.connect(db_conn)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_stripe_payments (
                    charge_id TEXT PRIMARY KEY,
                    amount INTEGER,
                    amount_captured INTEGER,
                    amount_refunded INTEGER,
                    currency TEXT,
                    customer_id TEXT,
                    description TEXT,
                    invoice_id TEXT,
                    payment_method TEXT,
                    receipt_email TEXT,
                    receipt_url TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    paid BOOLEAN,
                    refunded BOOLEAN,
                    captured BOOLEAN,
                    failure_code TEXT,
                    failure_message TEXT,
                    metadata JSONB,
                    extracted_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("TRUNCATE TABLE raw_stripe_payments")
            
            # Fetch charges
            charges = stripe.Charge.list(limit=100)
            
            payment_data = []
            for charge in charges.auto_paging_iter():
                payment_data.append((
                    charge.id, charge.amount, charge.amount_captured,
                    charge.amount_refunded, charge.currency, charge.customer,
                    charge.description, charge.invoice, charge.payment_method,
                    charge.receipt_email, charge.receipt_url, charge.status,
                    datetime.fromtimestamp(charge.created), charge.paid,
                    charge.refunded, charge.captured, charge.failure_code,
                    charge.failure_message, json.dumps(dict(charge.metadata))
                ))
                
                if len(payment_data) >= 100:
                    break
            
            if payment_data:
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw_stripe_payments 
                    (charge_id, amount, amount_captured, amount_refunded, currency, 
                     customer_id, description, invoice_id, payment_method, receipt_email,
                     receipt_url, status, created_at, paid, refunded, captured,
                     failure_code, failure_message, metadata)
                    VALUES %s
                    """,
                    payment_data
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✓ Inserted {len(payment_data)} payments")
            return {'payments_count': len(payment_data), 'skipped': False}
            
        except Exception as e:
            print(f"❌ Stripe payment extraction failed: {e}")
            return {'payments_count': 0, 'skipped': False, 'error': str(e)}
    
    @task()
    def extract_stripe_refunds(db_conn: str):
        """Extract refunds from Stripe API (optional - requires API key)"""
        stripe_key = os.getenv("STRIPE_API_KEY")
        
        if not stripe_key or stripe_key == "sk_test_YOUR_KEY_HERE":
            print("⚠️  No Stripe API key found. Skipping refund extraction.")
            return {'refunds_count': 0, 'skipped': True}
        
        print("Extracting refunds from Stripe...")
        
        try:
            import stripe
            stripe.api_key = stripe_key
            
            conn = psycopg2.connect(db_conn)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_stripe_refunds (
                    refund_id TEXT PRIMARY KEY,
                    charge_id TEXT,
                    amount INTEGER,
                    currency TEXT,
                    reason TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    receipt_number TEXT,
                    source_transfer_reversal TEXT,
                    transfer_reversal TEXT,
                    metadata JSONB,
                    extracted_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("TRUNCATE TABLE raw_stripe_refunds")
            
            refunds = stripe.Refund.list(limit=100)
            
            refund_data = []
            for refund in refunds.auto_paging_iter():
                refund_data.append((
                    refund.id, refund.charge, refund.amount, refund.currency,
                    refund.reason, refund.status,
                    datetime.fromtimestamp(refund.created), refund.receipt_number,
                    refund.source_transfer_reversal, refund.transfer_reversal,
                    json.dumps(dict(refund.metadata))
                ))
                
                if len(refund_data) >= 100:
                    break
            
            if refund_data:
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw_stripe_refunds 
                    (refund_id, charge_id, amount, currency, reason, status, 
                     created_at, receipt_number, source_transfer_reversal, 
                     transfer_reversal, metadata)
                    VALUES %s
                    """,
                    refund_data
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✓ Inserted {len(refund_data)} refunds")
            return {'refunds_count': len(refund_data), 'skipped': False}
            
        except Exception as e:
            print(f"❌ Stripe refund extraction failed: {e}")
            return {'refunds_count': 0, 'skipped': False, 'error': str(e)}
    
    @task()
    def extract_stripe_invoices(db_conn: str):
        """Extract invoices from Stripe API (optional - requires API key)"""
        stripe_key = os.getenv("STRIPE_API_KEY")
        
        if not stripe_key or stripe_key == "sk_test_YOUR_KEY_HERE":
            print("⚠️  No Stripe API key found. Skipping invoice extraction.")
            return {'invoices_count': 0, 'skipped': True}
        
        print("Extracting invoices from Stripe...")
        
        try:
            import stripe
            stripe.api_key = stripe_key
            
            conn = psycopg2.connect(db_conn)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_stripe_invoices (
                    invoice_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    subscription_id TEXT,
                    amount_due INTEGER,
                    amount_paid INTEGER,
                    amount_remaining INTEGER,
                    currency TEXT,
                    description TEXT,
                    invoice_pdf TEXT,
                    hosted_invoice_url TEXT,
                    number TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    due_date TIMESTAMP,
                    period_start TIMESTAMP,
                    period_end TIMESTAMP,
                    paid BOOLEAN,
                    attempted BOOLEAN,
                    metadata JSONB,
                    extracted_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_stripe_invoice_items (
                    item_id TEXT PRIMARY KEY,
                    invoice_id TEXT,
                    amount INTEGER,
                    currency TEXT,
                    description TEXT,
                    quantity INTEGER,
                    unit_amount INTEGER,
                    extracted_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("TRUNCATE TABLE raw_stripe_invoices CASCADE")
            cursor.execute("TRUNCATE TABLE raw_stripe_invoice_items")
            
            invoices = stripe.Invoice.list(limit=100)
            
            invoice_data = []
            invoice_items_data = []
            
            for invoice in invoices.auto_paging_iter():
                invoice_data.append((
                    invoice.id, invoice.customer, invoice.subscription,
                    invoice.amount_due, invoice.amount_paid, invoice.amount_remaining,
                    invoice.currency, invoice.description, invoice.invoice_pdf,
                    invoice.hosted_invoice_url, invoice.number, invoice.status,
                    datetime.fromtimestamp(invoice.created),
                    datetime.fromtimestamp(invoice.due_date) if invoice.due_date else None,
                    datetime.fromtimestamp(invoice.period_start),
                    datetime.fromtimestamp(invoice.period_end),
                    invoice.paid, invoice.attempted, json.dumps(dict(invoice.metadata))
                ))
                
                for line_item in invoice.lines.data:
                    invoice_items_data.append((
                        line_item.id, invoice.id, line_item.amount,
                        line_item.currency, line_item.description,
                        line_item.quantity, line_item.unit_amount
                    ))
                
                if len(invoice_data) >= 100:
                    break
            
            if invoice_data:
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw_stripe_invoices 
                    (invoice_id, customer_id, subscription_id, amount_due, amount_paid,
                     amount_remaining, currency, description, invoice_pdf, hosted_invoice_url,
                     number, status, created_at, due_date, period_start, period_end,
                     paid, attempted, metadata)
                    VALUES %s
                    """,
                    invoice_data
                )
            
            if invoice_items_data:
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw_stripe_invoice_items 
                    (item_id, invoice_id, amount, currency, description, 
                     quantity, unit_amount)
                    VALUES %s
                    """,
                    invoice_items_data
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✓ Inserted {len(invoice_data)} invoices and {len(invoice_items_data)} line items")
            return {
                'invoices_count': len(invoice_data),
                'invoice_items_count': len(invoice_items_data),
                'skipped': False
            }
            
        except Exception as e:
            print(f"❌ Stripe invoice extraction failed: {e}")
            return {'invoices_count': 0, 'skipped': False, 'error': str(e)}
    
    @task()
    def log_extraction_summary(
        products: dict,
        users: dict,
        carts: dict,
        orders: dict,
        payments: dict,
        refunds: dict,
        invoices: dict
    ):
        """Log summary of extraction results"""
        print("=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Products:       {products.get('products_count', 0)}")
        print(f"Users:          {users.get('users_count', 0)}")
        print(f"Carts:          {carts.get('carts_count', 0)}")
        print(f"Cart Items:     {carts.get('cart_items_count', 0)}")
        print(f"Orders:         {orders.get('orders_count', 0)}")
        print("-" * 60)
        
        if payments.get('skipped'):
            print("Stripe Payments: SKIPPED (no API key)")
        else:
            print(f"Stripe Payments: {payments.get('payments_count', 0)}")
        
        if refunds.get('skipped'):
            print("Stripe Refunds:  SKIPPED (no API key)")
        else:
            print(f"Stripe Refunds:  {refunds.get('refunds_count', 0)}")
        
        if invoices.get('skipped'):
            print("Stripe Invoices: SKIPPED (no API key)")
        else:
            print(f"Stripe Invoices: {invoices.get('invoices_count', 0)}")
        
        print("=" * 60)
        print("✓ EXTRACTION COMPLETE")
        print("=" * 60)
        
        return {
            'dummyjson': {
                'products': products.get('products_count', 0),
                'users': users.get('users_count', 0),
                'carts': carts.get('carts_count', 0),
                'orders': orders.get('orders_count', 0),
            },
            'stripe': {
                'payments': payments.get('payments_count', 0),
                'refunds': refunds.get('refunds_count', 0),
                'invoices': invoices.get('invoices_count', 0),
            }
        }
    
    # Define task dependencies
    db_conn = get_db_connection()
    
    # DummyJSON extractions (run in parallel)
    products_result = extract_products(db_conn)
    users_result = extract_users(db_conn)
    carts_result = extract_carts(db_conn)
    
    # Orders derived from carts
    orders_result = derive_orders(db_conn, carts_result)
    
    # Stripe extractions (run in parallel, optional)
    payments_result = extract_stripe_payments(db_conn)
    refunds_result = extract_stripe_refunds(db_conn)
    invoices_result = extract_stripe_invoices(db_conn)
    
    # Log summary (waits for all tasks)
    summary = log_extraction_summary(
        products_result,
        users_result,
        carts_result,
        orders_result,
        payments_result,
        refunds_result,
        invoices_result
    )


# Instantiate the DAG
dag_instance = ecommerce_extraction_pipeline()
