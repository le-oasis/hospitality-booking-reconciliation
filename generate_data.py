"""
Generate realistic hospitality booking data for GA4 and Salesforce
Simulates common discrepancies found in real-world scenarios
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# ============================================================================
# CONFIGURATION
# ============================================================================
NUM_BOOKINGS = 500
START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2025, 10, 24)

HOTELS = [
    'Grand Plaza Hotel NYC',
    'Sunset Resort Miami',
    'Mountain View Lodge Denver',
    'Coastal Inn San Diego'
]

UTM_SOURCES = ['google', 'facebook', 'instagram', 'email', 'direct', None]
UTM_CAMPAIGNS = ['summer_sale', 'last_minute', 'premium_rooms', 'family_package', None]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def random_date(start, end):
    """Generate random datetime between start and end"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start + timedelta(days=random_days, seconds=random_seconds)

def generate_gclid():
    """Generate fake Google Click ID"""
    return f"Cj0KCQjw{random.randint(1000, 9999)}QAvD_BwE"

def generate_transaction_id():
    """Generate booking confirmation number"""
    return f"BK{random.randint(100000, 999999)}"

# ============================================================================
# GENERATE BASE BOOKINGS (GROUND TRUTH)
# ============================================================================

base_bookings = []

for i in range(NUM_BOOKINGS):
    booking_datetime = random_date(START_DATE, END_DATE)
    transaction_id = generate_transaction_id()
    
    # Booking details
    hotel = random.choice(HOTELS)
    nights = random.randint(1, 7)
    check_in_date = booking_datetime + timedelta(days=random.randint(7, 60))
    check_out_date = check_in_date + timedelta(days=nights)
    
    # Pricing
    room_rate = random.randint(150, 500)
    booking_value = room_rate * nights
    
    # Attribution
    utm_source = random.choice(UTM_SOURCES)
    utm_campaign = random.choice(UTM_CAMPAIGNS) if utm_source else None
    gclid = generate_gclid() if utm_source == 'google' else None
    
    # User info
    user_id = f"user_{random.randint(1000, 9999)}"
    
    base_bookings.append({
        'transaction_id': transaction_id,
        'booking_datetime': booking_datetime,
        'user_id': user_id,
        'hotel': hotel,
        'check_in_date': check_in_date,
        'check_out_date': check_out_date,
        'nights': nights,
        'room_rate': room_rate,
        'booking_value': booking_value,
        'utm_source': utm_source,
        'utm_campaign': utm_campaign,
        'gclid': gclid
    })

base_df = pd.DataFrame(base_bookings)

# ============================================================================
# CREATE GA4 EVENTS (WITH REALISTIC ISSUES)
# ============================================================================

ga4_events = []

for _, booking in base_df.iterrows():
    # 95% of bookings make it to GA4
    if random.random() < 0.95:
        
        # Simulate GA4 event structure
        event_params = []
        
        # Transaction ID
        event_params.append({
            'key': 'transaction_id',
            'value': {'string_value': booking['transaction_id']}
        })
        
        # Booking value
        event_params.append({
            'key': 'value',
            'value': {'double_value': float(booking['booking_value'])}
        })
        
        # GCLID (90% success rate even when it exists)
        if booking['gclid'] and random.random() < 0.90:
            event_params.append({
                'key': 'gclid',
                'value': {'string_value': booking['gclid']}
            })
        
        # UTM parameters
        if booking['utm_source']:
            event_params.append({
                'key': 'utm_source',
                'value': {'string_value': booking['utm_source']}
            })
        if booking['utm_campaign']:
            event_params.append({
                'key': 'utm_campaign',
                'value': {'string_value': booking['utm_campaign']}
            })
        
        ga4_events.append({
            'event_date': booking['booking_datetime'].strftime('%Y%m%d'),
            'event_timestamp': int(booking['booking_datetime'].timestamp() * 1000000),
            'event_name': 'purchase',
            'user_pseudo_id': booking['user_id'],
            'event_params': event_params
        })

# Add some TEST bookings (internal team testing - in GA4 but not real)
for _ in range(10):
    test_datetime = random_date(START_DATE, END_DATE)
    ga4_events.append({
        'event_date': test_datetime.strftime('%Y%m%d'),
        'event_timestamp': int(test_datetime.timestamp() * 1000000),
        'event_name': 'purchase',
        'user_pseudo_id': 'test_user_internal',
        'event_params': [
            {'key': 'transaction_id', 'value': {'string_value': f"TEST{random.randint(1000, 9999)}"}},
            {'key': 'value', 'value': {'double_value': 99.99}}
        ]
    })

ga4_df = pd.DataFrame(ga4_events)

# ============================================================================
# CREATE SALESFORCE OPPORTUNITIES (WITH REALISTIC ISSUES)
# ============================================================================

sf_opportunities = []

for _, booking in base_df.iterrows():
    
    # 10% of bookings are cancelled within 1 hour (before hitting Salesforce)
    if random.random() < 0.10:
        continue
    
    # 92% make it to Salesforce
    if random.random() < 0.92:
        
        # Salesforce records opportunity 5-15 minutes after booking
        sf_datetime = booking['booking_datetime'] + timedelta(minutes=random.randint(5, 15))
        
        # GCLID sometimes lost (85% success rate)
        gclid_value = booking['gclid'] if booking['gclid'] and random.random() < 0.85 else None
        
        sf_opportunities.append({
            'opportunity_id': str(uuid.uuid4()),
            'confirmation_number': booking['transaction_id'],
            'created_date': sf_datetime,
            'property_name': booking['hotel'],
            'check_in_date': booking['check_in_date'],
            'check_out_date': booking['check_out_date'],
            'nights': booking['nights'],
            'amount': booking['booking_value'],
            'stage_name': random.choice(['Booked', 'Confirmed', 'Confirmed', 'Confirmed']),  # Most are confirmed
            'booking_source': booking['utm_source'] if booking['utm_source'] else 'direct',
            'gclid__c': gclid_value,
            'utm_source__c': booking['utm_source'],
            'utm_campaign__c': booking['utm_campaign']
        })

# Add PHONE bookings (in Salesforce but not in GA4)
for _ in range(30):
    phone_datetime = random_date(START_DATE, END_DATE)
    sf_opportunities.append({
        'opportunity_id': str(uuid.uuid4()),
        'confirmation_number': generate_transaction_id(),
        'created_date': phone_datetime,
        'property_name': random.choice(HOTELS),
        'check_in_date': phone_datetime + timedelta(days=random.randint(7, 30)),
        'check_out_date': phone_datetime + timedelta(days=random.randint(8, 37)),
        'nights': random.randint(1, 5),
        'amount': random.randint(300, 1500),
        'stage_name': 'Confirmed',
        'booking_source': 'phone',
        'gclid__c': None,
        'utm_source__c': None,
        'utm_campaign__c': None
    })

sf_df = pd.DataFrame(sf_opportunities)

# ============================================================================
# SAVE TO CSV
# ============================================================================

# Save GA4 events
ga4_df.to_csv('/opt/airflow/data/raw_ga4_events.csv', index=False)

# Save Salesforce opportunities  
sf_df.to_csv('/opt/airflow/data/raw_salesforce_opportunities.csv', index=False)

# Save base truth for validation
base_df.to_csv('/opt/airflow/data/base_truth_bookings.csv', index=False)

# ============================================================================
# PRINT SUMMARY
# ============================================================================

print("=" * 80)
print("DATA GENERATION COMPLETE")
print("=" * 80)
print(f"\n📊 BASE BOOKINGS: {len(base_df)}")
print(f"📊 GA4 EVENTS: {len(ga4_df)}")
print(f"📊 SALESFORCE OPPORTUNITIES: {len(sf_df)}")

print(f"\n💡 EXPECTED DISCREPANCIES:")
print(f"   - GA4 events with GCLID: {len([e for e in ga4_events if any(p['key'] == 'gclid' for p in e.get('event_params', []))])}")
print(f"   - SF opportunities with GCLID: {len(sf_df[sf_df['gclid__c'].notna()])}")
print(f"   - Test bookings in GA4: ~10")
print(f"   - Phone bookings (SF only): ~30")
print(f"   - Quick cancellations: ~{int(len(base_df) * 0.10)}")

print(f"\n📁 Files saved to /opt/airflow/data/")
print("   - raw_ga4_events.csv")
print("   - raw_salesforce_opportunities.csv")
print("   - base_truth_bookings.csv")
