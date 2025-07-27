#!/usr/bin/env python3
"""
MongoDB Data Seeding Script
Generates fake data for an e-commerce database using Faker and PyMongo
"""

import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

# Initialize Faker instance
fake = Faker()

def connect_to_mongodb():
    """Connect to MongoDB instance"""
    try:
        # Use directConnection=True to avoid replica set member discovery issues
        client = MongoClient('mongodb://localhost:27017/?directConnection=true')
        # Test the connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        raise

def generate_customers(db, count=50):
    """Generate and insert fake customer data"""
    print(f"🔄 Generating {count} customers...")
    
    customers_collection = db.customers
    customers = []
    
    for _ in range(count):
        customer = {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'registration_date': fake.date_time_between(start_date='-2y', end_date='now')
        }
        customers.append(customer)
    
    # Insert customers
    result = customers_collection.insert_many(customers)
    customer_ids = result.inserted_ids
    
    print(f"✅ Inserted {len(customer_ids)} customers")
    return customer_ids

def generate_products(db, count=100):
    """Generate and insert fake product data"""
    print(f"🔄 Generating {count} products...")
    
    products_collection = db.products
    products = []
    
    # Product categories for more realistic names
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty']
    
    for _ in range(count):
        category = random.choice(categories)
        product = {
            'name': f"{fake.word().title()} {category[:-1]}" if category.endswith('s') else f"{fake.word().title()} {category}",
            'price': round(random.uniform(10.0, 500.0), 2),
            'stock_quantity': random.randint(0, 100)
        }
        products.append(product)
    
    # Insert products
    result = products_collection.insert_many(products)
    product_ids = result.inserted_ids
    
    print(f"✅ Inserted {len(product_ids)} products")
    return product_ids

def generate_orders(db, customer_ids, product_ids, count=200):
    """Generate and insert fake order data"""
    print(f"🔄 Generating {count} orders...")
    
    orders_collection = db.orders
    orders = []
    
    statuses = ['pending', 'shipped', 'delivered']
    
    for _ in range(count):
        # Random customer
        customer_id = random.choice(customer_ids)
        
        # Random order date (within last year)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        # Random status
        status = random.choice(statuses)
        
        # Generate line items (1-5 items per order)
        num_items = random.randint(1, 5)
        line_items = []
        
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            price = round(random.uniform(10.0, 500.0), 2)
            
            line_item = {
                'product_id': product_id,
                'quantity': quantity,
                'price': price
            }
            line_items.append(line_item)
        
        order = {
            'customer_id': customer_id,
            'order_date': order_date,
            'status': status,
            'line_items': line_items
        }
        orders.append(order)
    
    # Insert orders
    result = orders_collection.insert_many(orders)
    order_ids = result.inserted_ids
    
    print(f"✅ Inserted {len(order_ids)} orders")
    return order_ids

def print_collection_stats(db):
    """Print statistics about the collections"""
    print("\n📊 Collection Statistics:")
    print("-" * 30)
    
    collections = ['customers', 'products', 'orders']
    for collection_name in collections:
        count = db[collection_name].count_documents({})
        print(f"{collection_name.capitalize()}: {count} documents")
    
    # Sample data from each collection
    print("\n📋 Sample Data:")
    print("-" * 30)
    
    print("\nSample Customer:")
    sample_customer = db.customers.find_one()
    if sample_customer:
        print(f"  Name: {sample_customer.get('first_name')} {sample_customer.get('last_name')}")
        print(f"  Email: {sample_customer.get('email')}")
        print(f"  Registration: {sample_customer.get('registration_date')}")
    
    print("\nSample Product:")
    sample_product = db.products.find_one()
    if sample_product:
        print(f"  Name: {sample_product.get('name')}")
        print(f"  Price: ${sample_product.get('price')}")
        print(f"  Stock: {sample_product.get('stock_quantity')}")
    
    print("\nSample Order:")
    sample_order = db.orders.find_one()
    if sample_order:
        print(f"  Customer ID: {sample_order.get('customer_id')}")
        print(f"  Date: {sample_order.get('order_date')}")
        print(f"  Status: {sample_order.get('status')}")
        print(f"  Line Items: {len(sample_order.get('line_items', []))}")

def main():
    """Main function to orchestrate data seeding"""
    print("🚀 Starting MongoDB data seeding process...")
    print("=" * 50)
    
    try:
        # Connect to MongoDB
        client = connect_to_mongodb()
        db = client.ecom
        
        print(f"📁 Using database: {db.name}")
        
        # Clear existing data (optional - comment out if you want to keep existing data)
        print("🧹 Clearing existing collections...")
        db.customers.delete_many({})
        db.products.delete_many({})
        db.orders.delete_many({})
        
        # Generate data
        customer_ids = generate_customers(db, 50)
        product_ids = generate_products(db, 100)
        order_ids = generate_orders(db, customer_ids, product_ids, 200)
        
        # Print statistics
        print_collection_stats(db)
        
        print("\n🎉 Data seeding completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during data seeding: {e}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            print("🔌 MongoDB connection closed")

if __name__ == "__main__":
    main() 