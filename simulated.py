import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple
import json

class LargeScaleDataGenerator:
    """Generate large-scale realistic data for business scenarios"""
    
    def __init__(self):
        self.setup_business_data()
    
    def setup_business_data(self):
        """Initialize business-specific data"""
        
        # Product categories and realistic SKUs
        self.product_categories = {
            'Interior Paint': {
                'brands': ['ProClassic', 'SuperPaint', 'Cashmere', 'Duration Home', 'Showcase'],
                'sheens': ['Flat', 'Eggshell', 'Satin', 'Semi-Gloss', 'Gloss'],
                'sizes': ['Quart', 'Gallon', '5-Gallon'],
                'price_range': (25, 85),
                'seasonal_factor': 1.2  # Higher demand in spring/summer
            },
            'Exterior Paint': {
                'brands': ['Duration Exterior', 'Resilience', 'SuperDeck', 'Woodscapes'],
                'sheens': ['Flat', 'Satin', 'Semi-Gloss', 'Gloss'],
                'sizes': ['Quart', 'Gallon', '5-Gallon'],
                'price_range': (30, 95),
                'seasonal_factor': 1.8  # Peak in spring/summer
            },
            'Stains & Finishes': {
                'brands': ['Minwax', 'Thompson\'s WaterSeal', 'Woodscapes', 'ProMar'],
                'sheens': ['Clear', 'Semi-Transparent', 'Solid'],
                'sizes': ['Quart', 'Gallon'],
                'price_range': (20, 75),
                'seasonal_factor': 1.4
            },
            'Primers': {
                'brands': ['ProBlock', 'Extreme Block', 'MultiPurpose', 'Oil-Based'],
                'sheens': ['Flat'],
                'sizes': ['Quart', 'Gallon', '5-Gallon'],
                'price_range': (15, 60),
                'seasonal_factor': 1.0
            },
            'Tools & Supplies': {
                'brands': ['Premium', 'Professional', 'Standard', 'Contractor Grade'],
                'sheens': ['N/A'],
                'sizes': ['Each', 'Pack', 'Case'],
                'price_range': (5, 150),
                'seasonal_factor': 1.1
            }
        }
        
        # Store types and characteristics
        self.store_types = {
            'Company Store': {
                'count': 4800,
                'avg_daily_transactions': 85,
                'avg_ticket_size': 120,
                'contractor_ratio': 0.6
            },
            'Franchise': {
                'count': 1200,
                'avg_daily_transactions': 45,
                'avg_ticket_size': 95,
                'contractor_ratio': 0.4
            },
            'Home Center': {
                'count': 15000,
                'avg_daily_transactions': 200,
                'avg_ticket_size': 65,
                'contractor_ratio': 0.2
            }
        }
        
        # Geographic regions with different characteristics
        self.regions = {
            'Northeast': {'stores': 1800, 'seasonal_multiplier': 0.6, 'avg_price_premium': 1.15},
            'Southeast': {'stores': 2200, 'seasonal_multiplier': 0.8, 'avg_price_premium': 0.95},
            'Midwest': {'stores': 2500, 'seasonal_multiplier': 0.5, 'avg_price_premium': 1.0},
            'Southwest': {'stores': 2000, 'seasonal_multiplier': 0.9, 'avg_price_premium': 1.05},
            'West': {'stores': 1800, 'seasonal_multiplier': 0.7, 'avg_price_premium': 1.25},
            'Northwest': {'stores': 1200, 'seasonal_multiplier': 0.4, 'avg_price_premium': 1.10}
        }
        
        # Customer segments
        self.customer_segments = {
            'DIY Homeowner': {'ratio': 0.45, 'avg_ticket': 85, 'frequency': 2.5},
            'Professional Contractor': {'ratio': 0.35, 'avg_ticket': 350, 'frequency': 12.0},
            'Property Manager': {'ratio': 0.15, 'avg_ticket': 280, 'frequency': 8.0},
            'Commercial': {'ratio': 0.05, 'avg_ticket': 850, 'frequency': 6.0}
        }

    def generate_store_data(self, num_stores: int = 12000) -> pd.DataFrame:
        """Generate store master data"""
        
        stores = []
        store_id = 1
        
        for region, region_data in self.regions.items():
            region_stores = int(num_stores * region_data['stores'] / sum(r['stores'] for r in self.regions.values()))
            
            for store_type, type_data in self.store_types.items():
                type_count = int(region_stores * type_data['count'] / sum(t['count'] for t in self.store_types.values()))
                
                for _ in range(type_count):
                    stores.append({
                        'store_id': store_id,
                        'store_type': store_type,
                        'region': region,
                        'state': self._get_random_state_for_region(region),
                        'city': f'City_{store_id}',
                        'zip_code': f'{random.randint(10000, 99999)}',
                        'square_footage': random.randint(3000, 12000),
                        'opening_date': self._random_date(datetime(2010, 1, 1), datetime(2023, 1, 1)),
                        'manager_id': f'MGR_{random.randint(1000, 9999)}',
                        'phone': f'{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}'
                    })
                    store_id += 1
        
        return pd.DataFrame(stores)

    def generate_product_catalog(self, num_products: int = 50000) -> pd.DataFrame:
        """Generate comprehensive product catalog"""
        
        products = []
        sku_id = 100000
        
        for category, cat_data in self.product_categories.items():
            category_products = int(num_products * self._get_category_weight(category))
            
            for _ in range(category_products):
                brand = random.choice(cat_data['brands'])
                sheen = random.choice(cat_data['sheens'])
                size = random.choice(cat_data['sizes'])
                
                # Generate realistic product names
                color_name = self._generate_color_name() if 'Paint' in category else ''
                product_name = f"{brand} {category} {color_name} {sheen} {size}".strip()
                
                base_price = random.uniform(*cat_data['price_range'])
                
                products.append({
                    'sku': f'P{sku_id}',
                    'product_name': product_name,
                    'category': category,
                    'brand': brand,
                    'sheen': sheen,
                    'size': size,
                    'color_name': color_name,
                    'base_price': round(base_price, 2),
                    'cost': round(base_price * random.uniform(0.4, 0.7), 2),
                    'weight_lbs': self._get_product_weight(size),
                    'is_hazmat': category in ['Stains & Finishes', 'Primers'],
                    'vendor_id': f'VND_{random.randint(100, 999)}',
                    'launch_date': self._random_date(datetime(2015, 1, 1), datetime(2024, 1, 1))
                })
                sku_id += 1
        
        return pd.DataFrame(products)

    def generate_transactions_chunk(self, chunk_id: int, records_per_chunk: int, 
                                  stores_df: pd.DataFrame, products_df: pd.DataFrame,
                                  start_date: datetime, end_date: datetime, 
                                  base_path: str) -> str:
        """Generate a chunk of transaction data - designed for parallel execution"""
        
        np.random.seed(chunk_id)
        random.seed(chunk_id)
        
        transactions = []
        transaction_id = chunk_id * records_per_chunk + 1000000
        
        # Pre-calculate some lookup data
        stores_list = stores_df.to_dict('records')
        products_list = products_df.to_dict('records')
        
        days_range = (end_date - start_date).days
        
        for _ in range(records_per_chunk):
            # Select random store and get its characteristics
            store = random.choice(stores_list)
            store_type_data = self.store_types[store['store_type']]
            region_data = self.regions[store['region']]
            
            # Generate realistic timestamp with business patterns
            transaction_date = start_date + timedelta(days=random.randint(0, days_range))
            
            # Business hours weighting
            hour_weights = [0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.5, 1.0, 1.2, 1.5, 
                          1.8, 2.0, 2.2, 2.0, 1.8, 1.5, 1.2, 1.0, 0.8, 0.5, 0.3, 0.2, 0.1, 0.1]
            hour = np.random.choice(24, p=np.array(hour_weights)/sum(hour_weights))
            minute = random.randint(0, 59)
            
            timestamp = transaction_date.replace(hour=hour, minute=minute)
            
            # Apply seasonal factors
            seasonal_multiplier = self._get_seasonal_multiplier(transaction_date.month)
            
            # Customer segment selection
            segment = np.random.choice(
                list(self.customer_segments.keys()),
                p=[seg['ratio'] for seg in self.customer_segments.values()]
            )
            segment_data = self.customer_segments[segment]
            
            # Generate realistic customer ID (repeat customers)
            customer_id = self._generate_customer_id(segment, chunk_id)
            
            # Transaction value influenced by multiple factors
            base_ticket = segment_data['avg_ticket']
            ticket_multiplier = (
                seasonal_multiplier * 
                region_data['seasonal_multiplier'] * 
                random.uniform(0.3, 2.5)
            )
            transaction_total = base_ticket * ticket_multiplier
            
            # Select products for this transaction
            num_items = self._get_realistic_item_count(segment, transaction_total)
            transaction_items = []
            running_total = 0
            
            for item_num in range(num_items):
                product = random.choice(products_list)
                
                # Adjust price based on region and promotions
                adjusted_price = product['base_price'] * region_data['avg_price_premium']
                
                # Random promotions/discounts
                if random.random() < 0.15:  # 15% chance of discount
                    discount_pct = random.uniform(0.05, 0.25)
                    adjusted_price *= (1 - discount_pct)
                
                quantity = self._get_realistic_quantity(product['category'], segment)
                line_total = adjusted_price * quantity
                running_total += line_total
                
                transaction_items.append({
                    'transaction_id': transaction_id,
                    'item_sequence': item_num + 1,
                    'sku': product['sku'],
                    'quantity': quantity,
                    'unit_price': round(adjusted_price, 2),
                    'line_total': round(line_total, 2),
                    'discount_amount': round(product['base_price'] * region_data['avg_price_premium'] - adjusted_price, 2) * quantity
                })
            
            # Main transaction record
            transactions.append({
                'transaction_id': transaction_id,
                'store_id': store['store_id'],
                'customer_id': customer_id,
                'customer_segment': segment,
                'timestamp': timestamp,
                'date': transaction_date.date(),
                'hour': hour,
                'day_of_week': transaction_date.weekday(),
                'month': transaction_date.month,
                'quarter': (transaction_date.month - 1) // 3 + 1,
                'year': transaction_date.year,
                'subtotal': round(running_total, 2),
                'tax_amount': round(running_total * 0.08, 2),  # 8% avg tax
                'total_amount': round(running_total * 1.08, 2),
                'payment_method': np.random.choice(['Credit', 'Cash', 'Check', 'Account'], 
                                                 p=[0.6, 0.25, 0.1, 0.05]),
                'employee_id': f'EMP_{random.randint(1000, 9999)}',
                'is_return': random.random() < 0.02,  # 2% return rate
                'loyalty_member': random.random() < 0.4,  # 40% loyalty members
                'channel': 'In-Store'  # Can be extended for online
            })
            
            transaction_id += 1
            
            # Save items to separate file for this chunk
            if transaction_items:
                items_df = pd.DataFrame(transaction_items)
                items_path = f"{base_path}/transaction_items_chunk_{chunk_id:04d}.parquet"
                items_df.to_parquet(items_path, compression='snappy', index=False)
        
        # Save transaction headers
        trans_df = pd.DataFrame(transactions)
        trans_path = f"{base_path}/transactions_chunk_{chunk_id:04d}.parquet"
        trans_df.to_parquet(trans_path, compression='snappy', index=False)
        
        return f"Generated chunk {chunk_id}: {len(transactions):,} transactions, {len(transaction_items):,} items"

    def generate_massive_dataset(self, 
                                       total_transactions: int = 500_000_000,  # 500M transactions
                                       base_path: str = "./simulated_data",
                                       start_date: datetime = datetime(2020, 1, 1),
                                       end_date: datetime = datetime(2024, 12, 31)) -> Dict:
        """Generate massive dataset with realistic business patterns"""
        
        print("🎨 Generating Large-Scale Dataset")
        print(f"Target: {total_transactions:,} transactions (~{total_transactions * 0.1 / 1000000:.0f}GB estimated)")
        
        os.makedirs(base_path, exist_ok=True)
        
        # Generate master data first
        print("Generating master data...")
        stores_df = self.generate_store_data(12000)
        products_df = self.generate_product_catalog(50000)
        
        # Save master data
        stores_df.to_parquet(f"{base_path}/stores_master.parquet", index=False)
        products_df.to_parquet(f"{base_path}/products_master.parquet", index=False)
        
        print(f"✓ Stores: {len(stores_df):,}")
        print(f"✓ Products: {len(products_df):,}")
        
        # Generate transactions in parallel chunks
        transactions_per_chunk = 1_000_000  # 1M transactions per chunk
        num_chunks = total_transactions // transactions_per_chunk
        
        print(f"\nGenerating {total_transactions:,} transactions in {num_chunks} chunks...")
        print(f"Each chunk: {transactions_per_chunk:,} transactions")
        
        # Use ProcessPoolExecutor for true parallelism
        with ProcessPoolExecutor(max_workers=min(os.cpu_count(), 16)) as executor:
            futures = []
            for chunk_id in range(num_chunks):
                future = executor.submit(
                    self.generate_transactions_chunk,
                    chunk_id, transactions_per_chunk, stores_df, products_df,
                    start_date, end_date, base_path
                )
                futures.append(future)
            
            # Monitor progress
            completed = 0
            for future in futures:
                result = future.result()
                completed += 1
                print(f"Progress: {completed}/{num_chunks} - {result}")
        
        # Generate summary statistics
        dataset_info = {
            'total_transactions': total_transactions,
            'total_stores': len(stores_df),
            'total_products': len(products_df),
            'date_range': f"{start_date.date()} to {end_date.date()}",
            'estimated_size_gb': total_transactions * 0.1 / 1000000,
            'chunks_generated': num_chunks,
            'base_path': base_path
        }
        
        # Save metadata
        with open(f"{base_path}/dataset_info.json", 'w') as f:
            json.dump(dataset_info, f, indent=2, default=str)
        
        print(f"\n🎉 Dataset generation complete!")
        print(f"📊 Total transactions: {total_transactions:,}")
        print(f"📁 Data location: {base_path}")
        print(f"💾 Estimated size: {dataset_info['estimated_size_gb']:.1f} GB")
        
        return dataset_info

    # Helper methods
    def _get_category_weight(self, category: str) -> float:
        """Get relative weight for product category"""
        weights = {
            'Interior Paint': 0.4,
            'Exterior Paint': 0.25,
            'Stains & Finishes': 0.15,
            'Primers': 0.1,
            'Tools & Supplies': 0.1
        }
        return weights.get(category, 0.02)

    def _generate_color_name(self) -> str:
        """Generate realistic paint color names"""
        colors = ['White', 'Beige', 'Gray', 'Blue', 'Green', 'Red', 'Yellow', 'Brown', 'Black']
        descriptors = ['Pure', 'Antique', 'Natural', 'Classic', 'Modern', 'Creamy', 'Deep', 'Light']
        return f"{random.choice(descriptors)} {random.choice(colors)}"

    def _get_product_weight(self, size: str) -> float:
        """Get realistic product weight based on size"""
        weights = {
            'Quart': random.uniform(2.5, 3.5),
            'Gallon': random.uniform(10, 12),
            '5-Gallon': random.uniform(50, 60),
            'Each': random.uniform(0.5, 5),
            'Pack': random.uniform(2, 8),
            'Case': random.uniform(15, 30)
        }
        return weights.get(size, 1.0)

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """Generate random date between start and end"""
        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)

    def _get_random_state_for_region(self, region: str) -> str:
        """Get realistic state for region"""
        states = {
            'Northeast': ['NY', 'NJ', 'PA', 'CT', 'MA', 'ME', 'VT', 'NH', 'RI'],
            'Southeast': ['FL', 'GA', 'NC', 'SC', 'VA', 'TN', 'KY', 'AL', 'MS'],
            'Midwest': ['IL', 'IN', 'OH', 'MI', 'WI', 'MN', 'IA', 'MO', 'KS'],
            'Southwest': ['TX', 'AZ', 'NM', 'OK', 'AR', 'LA'],
            'West': ['CA', 'NV', 'UT', 'CO', 'WY'],
            'Northwest': ['WA', 'OR', 'ID', 'MT', 'AK']
        }
        return random.choice(states.get(region, ['XX']))

    def _get_seasonal_multiplier(self, month: int) -> float:
        """Get seasonal demand multiplier by month"""
        # Paint sales typically peak in spring/summer
        multipliers = {
            1: 0.7, 2: 0.8, 3: 1.1, 4: 1.4, 5: 1.6, 6: 1.8,
            7: 1.7, 8: 1.5, 9: 1.2, 10: 1.0, 11: 0.8, 12: 0.9
        }
        return multipliers.get(month, 1.0)

    def _generate_customer_id(self, segment: str, chunk_id: int) -> str:
        """Generate realistic customer ID with repeat behavior"""
        # Different customer pools for different segments
        pools = {
            'DIY Homeowner': 50_000_000,  # Large pool, infrequent
            'Professional Contractor': 2_000_000,  # Smaller pool, frequent
            'Property Manager': 500_000,  # Small pool, regular
            'Commercial': 100_000  # Very small pool, regular
        }
        
        pool_size = pools.get(segment, 10_000_000)
        customer_num = random.randint(1, pool_size)
        return f"{segment[:3].upper()}_{customer_num:08d}"

    def _get_realistic_item_count(self, segment: str, transaction_total: float) -> int:
        """Get realistic number of items based on customer segment and transaction size"""
        base_items = {
            'DIY Homeowner': 2.5,
            'Professional Contractor': 8.0,
            'Property Manager': 6.0,
            'Commercial': 12.0
        }
        
        base = base_items.get(segment, 3.0)
        # Adjust based on transaction size
        size_factor = min(transaction_total / 100, 3.0)
        return max(1, int(np.random.poisson(base * size_factor)))

    def _get_realistic_quantity(self, category: str, segment: str) -> int:
        """Get realistic quantity based on product category and customer segment"""
        base_qty = {
            ('Interior Paint', 'DIY Homeowner'): 2,
            ('Interior Paint', 'Professional Contractor'): 5,
            ('Exterior Paint', 'DIY Homeowner'): 3,
            ('Exterior Paint', 'Professional Contractor'): 8,
            ('Tools & Supplies', 'DIY Homeowner'): 1,
            ('Tools & Supplies', 'Professional Contractor'): 3,
        }
        
        key = (category, segment)
        base = base_qty.get(key, 2)
        return max(1, int(np.random.poisson(base)))


# Usage example
if __name__ == "__main__":
    # Initialize generator
    generator = LargeScaleDataGenerator()
    
    # Generate massive dataset
    # This will create ~50GB of data with 500M transactions
    dataset_info = generator.generate_massive_dataset(
        total_transactions=100_000_000,  # Start smaller for testing: 100M transactions (~10GB)
        base_path="./simulated_data",
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2024, 12, 31)
    )
    
    print("\nDataset generation completed!")
    print(f"Files created in: {dataset_info['base_path']}")
    print(f"To process with Spark, use: spark.read.parquet('{dataset_info['base_path']}/transactions_chunk_*.parquet')")
