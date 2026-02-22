import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_luu_network_data(num_carriers=20):
    print(f"🏭 LUU Hub active! Generating {num_carriers} Load Carriers...")
    
    packages = []
    
    sites = [
        {'name': 'Erfurt', 'type': 'FC'},
        {'name': 'Mönchengladbach', 'type': 'FC'},
        {'name': 'Lahr', 'type': 'FC'},
        {'name': 'Verona', 'type': 'FC'},
        {'name': 'Szczecin', 'type': 'RC'},
        {'name': 'Lodz', 'type': 'RC'},
        {'name': 'Glowno', 'type': 'RC'}
    ]
    
    statuses = ['Dispatched', 'In Transit', 'Delayed', 'Arrived']

    # --- LEVEL 1: THE LOAD CARRIER (PALLET) LOOP ---
    for _ in range(num_carriers):
    # Create a 3-letter abbreviation of the origin site (e.g., Erfurt -> ERF)
        site = random.choice(sites)
       
        origin_abbr = site['name'][:3].upper()
        # Build the LPN: LPN-ERF-12345-LUU
        load_carrier_id = f"LPN-{origin_abbr}-{random.randint(10000, 99999)}-LUU"
        
  
        
        # 🚨 CHAOS: Messy Origin Site strings
        origin_site = site['name']
        if random.random() < 0.10:
            origin_site = f"   {origin_site}  "
            
        site_type = site['type']
        carrier_status = random.choice(statuses)

        # Timestamps (Stripped exactly to the second)
        dispatch = fake.date_time_between(start_date='-14d', end_date='now').replace(microsecond=0)
        
        arrived = None
        if carrier_status == 'Arrived':
            transit_time = timedelta(days=random.randint(1, 3), hours=random.randint(1, 23), minutes=random.randint(1, 59))
            arrived = (dispatch + transit_time).replace(microsecond=0)

        # 🚨 CHAOS: The Time Machine (Arrival before dispatch)
        if arrived and random.random() < 0.05:
            arrived = (dispatch - timedelta(hours=random.randint(1, 48))).replace(microsecond=0)

        carrier_comment = None
        if carrier_status == 'Delayed':
            carrier_comment = random.choice([' weather delay ', 'CUSTOMS_HOLD', 'truck_breakdown', '  missing paperwork'])
        else:
            carrier_comment = random.choice([' standard handling', 'ALL_GOOD', 'ok', '   '])

        # --- LEVEL 2: THE ITEMS INSIDE THE CARRIER LOOP ---
        num_items = random.randint(10, 25) # 10 to 25 items per pallet
        
        for _ in range(num_items):
            item_id = fake.unique.bothify(text='ITEM-#########')
            
            # Base Category & Quality
            if site_type == 'FC':
                category = random.choices(['Zalando SE', 'Expiring Beauty'], weights=[85, 15])[0]
                incoming_quality = 'A' if category == 'Zalando SE (Overstock)' else random.choices(['A', 'B'], weights=[70, 30])[0]
                
                # 🚨 CHAOS: The Mixed Pallet (Customer Return accidentally put in an FC pallet)
                if random.random() < 0.05:
                    category = 'Customer Return'
            else:
                category = random.choices(['Customer Return', 'ZFS Defect'], weights=[80, 20])[0]
                incoming_quality = random.choices(['A', 'B', 'C', 'D'], weights=[40, 30, 20, 10])[0]

            # Operator Assessment (Degradation)
            # If it hasn't arrived, it shouldn't be assessed yet...
            assessed_grade = None
            if carrier_status == 'Arrived':
                if incoming_quality == 'A':
                    assessed_grade = random.choices(['A', 'B', 'C'], weights=[80, 15, 5])[0]
                elif incoming_quality == 'B':
                    assessed_grade = random.choices(['B', 'C', 'D'], weights=[70, 20, 10])[0]
                elif incoming_quality == 'C':
                    assessed_grade = random.choices(['C', 'D'], weights=[80, 20])[0]
                else:
                    assessed_grade = 'D'
                    
                # 5% chance operator forgets to grade
                if random.random() < 0.05:
                    assessed_grade = None
            
            # 🚨 CHAOS: The Ghost Scan (Item assessed even though the truck is "In Transit")
            if carrier_status == 'In Transit' and random.random() < 0.05:
                assessed_grade = 'A' 

            # Quality Notes
            quality_note = None
            if assessed_grade in ['A', 'B']:
                quality_note = random.choice(['Mint condition', ' original packaging', 'MINOR SCUFFS', 'perfect'])
            if assessed_grade in ['C', 'D'] and random.random() < 0.05:
                quality_note = "system_error_note_added"

            # Routing Destination
            destination = 'Overstock Partners'
            if assessed_grade == 'A':
                destination = 'Outlet Stores'
            elif assessed_grade == 'B':
                destination = random.choice(['Outlet Stores', 'Overstock Partners'])
            elif assessed_grade in ['C', 'D'] and category == 'ZFS Defect' and random.random() < 0.2:
                destination = 'Recycle Partners'
            elif not assessed_grade:
                 destination = 'Pending Assessment'

            weight = round(random.uniform(0.1, 3.5), 2)
            # 🚨 CHAOS: Negative weight
            if random.random() < 0.05:
                weight = -abs(weight) 

            # Build the denormalized flat record
            packages.append({
                "load_carrier_id": load_carrier_id, # The Pallet
                "origin_site": origin_site,
                "site_type": site_type,
                "carrier_status": carrier_status,
                "dispatch_timestamp": dispatch,
                "arrived_timestamp": arrived,
                "carrier_comments": carrier_comment,
                "item_id": item_id,                 # The Item inside
                "item_category": category,
                "incoming_quality": incoming_quality,
                "assessed_grade": assessed_grade,
                "quality_note": quality_note,
                "weight_kg": weight,
                "routed_destination": destination
            })

    # 🚨 CHAOS: The Double Scan (Duplicate 5 items at the end)
    duplicates = random.sample(packages, 5)
    packages.extend(duplicates)
    
    df = pd.DataFrame(packages)
    file_name = "luu_inbound_data.csv"
    df.to_csv(file_name, index=False)
    print(f"✅ Success! Generated {len(packages)} items packed into {num_carriers} Load Carriers!")

if __name__ == "__main__":
    generate_luu_network_data(20)