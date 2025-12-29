"""Generate sample e-commerce data for Windows"""
import os
from datetime import datetime, timedelta
import random
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

BRONZE_DIR = "data\\bronze"  # Windows path
os.makedirs(BRONZE_DIR, exist_ok=True)

def generate_customers(n):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'registration_date': fake.date_between(
                start_date=datetime(2023,1,1), 
                end_date=datetime(2024,12,31)
            ),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'is_active': random.choice([True, True, True, False])
        })
    return pd.DataFrame(customers)

def generate_products(n):
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Toys']
    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        base_price = random.uniform(10, 500)
        products.append({
            'product_id': i,
            'product_name': fake.catch_phrase(),
            'category': category,
            'subcategory': f"{category}_{random.randint(1, 5)}",
            'brand': fake.company(),
            'cost_price': round(base_price * 0.6, 2),
            'list_price': round(base_price, 2),
            'current_price': round(base_price * random.uniform(0.8, 1.0), 2),
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 50),
            'is_active': random.choice([True, True, True, False]),
            'created_at': fake.date_between(
                start_date=datetime(2023,1,1), 
                end_date=datetime(2024,12,31)
            )
        })
    return pd.DataFrame(products)

def generate_orders(customers_df, n):
    orders = []
    customer_ids = customers_df[customers_df['is_active']]['customer_id'].tolist()
    for i in range(1, n + 1):
        order_date = fake.date_time_between(
            start_date=datetime(2023,1,1), 
            end_date=datetime(2024,12,31)
        )
        status = random.choices(
            ['completed', 'shipped', 'processing', 'cancelled'], 
            weights=[70, 15, 10, 5]
        )[0]
        orders.append({
            'order_id': i,
            'customer_id': random.choice(customer_ids),
            'order_date': order_date,
            'order_status': status,
            'shipping_address': fake.street_address(),
            'shipping_city': fake.city(),
            'shipping_state': fake.state_abbr(),
            'shipping_zip': fake.zipcode(),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal']),
            'shipping_cost': round(random.uniform(5, 25), 2) if status != 'cancelled' else 0,
            'tax_amount': 0,
            'discount_amount': round(random.uniform(0, 50), 2) if random.random() > 0.7 else 0,
            'created_at': order_date,
            'updated_at': order_date + timedelta(days=random.randint(0, 7))
        })
    return pd.DataFrame(orders)

def generate_order_items(orders_df, products_df):
    order_items = []
    item_id = 1
    for _, order in orders_df.iterrows():
        if order['order_status'] == 'cancelled':
            continue
        num_items = random.randint(1, 5)
        available_products = products_df[products_df['is_active']]
        selected_products = available_products.sample(n=min(num_items, len(available_products)))
        for _, product in selected_products.iterrows():
            quantity = random.randint(1, 3)
            unit_price = product['current_price']
            order_items.append({
                'order_item_id': item_id,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': round(quantity * unit_price, 2),
                'discount_amount': round(random.uniform(0, 10), 2) if random.random() > 0.8 else 0
            })
            item_id += 1
    return pd.DataFrame(order_items)

if __name__ == "__main__":
    print("Generating e-commerce sample data...")
    
    customers_df = generate_customers(NUM_CUSTOMERS)
    customers_df.to_csv(f"{BRONZE_DIR}\\customers.csv", index=False)
    print(f"Created {len(customers_df)} customers")
    
    products_df = generate_products(NUM_PRODUCTS)
    products_df.to_csv(f"{BRONZE_DIR}\\products.csv", index=False)
    print(f"Created {len(products_df)} products")
    
    orders_df = generate_orders(customers_df, NUM_ORDERS)
    orders_df.to_csv(f"{BRONZE_DIR}\\orders.csv", index=False)
    print(f"Created {len(orders_df)} orders")
    
    order_items_df = generate_order_items(orders_df, products_df)
    order_items_df.to_csv(f"{BRONZE_DIR}\\order_items.csv", index=False)
    print(f"Created {len(order_items_df)} order items")
    
    print(f"\nAll data saved to {BRONZE_DIR}\\")
    print(f"Total Revenue: ${order_items_df['line_total'].sum():,.2f}")