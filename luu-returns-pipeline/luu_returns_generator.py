import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Wake up our "Improv Actor"
fake = Faker()

def generate_luu_network_data(num_records=200):
    print(f"🏭 LUU Hub active! Generating {num_records} inbound items from the logistics network...")
    
    packages = []
    
    # Origin FCs and Categories
    origin_fcs = ['Erfurt (ERF)', 'Mönchengladbach (MG)', 'Lahr (LH)', 'Szczecin (POZ)']
    item_categories = ['ZFS Defect', 'Overstock', 'Expiring Beauty', 'Zalando SE']
    
    # Operator grades (None = Operator forgot to scan!)
    operator_grades = ['A', 'B', 'C', 'D', None] 

    for _ in range(num_records):
        # 1. Base Information
        item_id = fake.unique.bothify(text='ITEM-#########')
        origin_site = random.choice(origin_fcs)
        category = random.choice(item_categories)
        grade = random.choice(operator_grades)
        
        # 2. LUU Routing Logic
        if grade == 'A':
            destination = 'Outlet Stores'
        elif grade == 'B':
            destination = random.choice(['Outlet Stores', 'Overstock Partners'])
        elif grade in ['C', 'D']:
            if category == 'ZFS Defect' and grade == 'D' and random.random() < 0.2:
                destination = 'Recycle Partners'
            else:
                destination = 'Overstock Partners'
        else:
            destination = 'Buffer Area (Missing Grade)'

        # 3. Timestamps
        dispatch = fake.date_between(start_date='-14d', end_date='today')
        arrived = dispatch + timedelta(days=random.randint(1, 4))
        
        # 4. Weight
        weight = round(random.uniform(0.1, 3.5), 2)

        # ---------------------------------------------------------
        # 🚨 THE CHAOS INJECTION ZONE (For dbt testing later) 🚨
        # ---------------------------------------------------------
        
        chaos_roll = random.random()
        
        # ERROR 1: The Broken Scale (5% chance of negative weight)
        if chaos_roll < 0.05:
            weight = -abs(weight) 
            
        # ERROR 2: The Time Machine (5% chance arrival is BEFORE dispatch)
        elif 0.05 <= chaos_roll < 0.10:
            arrived = dispatch - timedelta(days=random.randint(1, 3))
            
        # ---------------------------------------------------------

        # 5. Build the record
        package = {
            "item_id": item_id,
            "origin_site": origin_site,
            "item_category": category,
            "operator_grade": grade,
            "routed_destination": destination,
            "weight_kg": weight,
            "dispatch_date_from_fc": dispatch,
            "arrived_date_at_luu": arrived 
        }
        packages.append(package)

    # ERROR 3: The Double Scan (Duplicate 5 random rows at the end)
    duplicates = random.sample(packages, 5)
    packages.extend(duplicates)
    
    # Note: Because of duplicates, we will end up with 205 rows instead of 200!

    # Hand to Pandas and save
    df = pd.DataFrame(packages)
    file_name = "luu_inbound_data.csv"
    df.to_csv(file_name, index=False)
    
    print(f"✅ Success! Sorted {len(packages)} items (including intentional errors) into {file_name}")

if __name__ == "__main__":
    generate_luu_network_data(200)