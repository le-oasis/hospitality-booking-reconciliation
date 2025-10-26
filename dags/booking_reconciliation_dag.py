"""
Hospitality Booking Reconciliation Pipeline
Demonstrates GA4 vs Salesforce discrepancy investigation

Author: Mahmud Oyinloye (Interview Demo)
Date: October 2025
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import duckdb
import json
import random
import uuid

default_args = {
    'owner': 'mahmud',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    'booking_reconciliation_pipeline',
    default_args=default_args,
    description='GA4 vs Salesforce booking reconciliation for hospitality client',
    schedule_interval='@daily',
    catchup=False,
    tags=['hospitality', 'reconciliation', 'interview-demo']
) as dag:

    # ========================================================================
    # TASK 0: GENERATE SAMPLE DATA (NEW - FIRST TASK)
    # ========================================================================

    @task(task_id='generate_sample_data')
    def generate_sample_data():
        """
        Generate realistic hospitality booking data for GA4 and Salesforce
        Simulates common discrepancies found in real-world scenarios
        """
        print("🎲 Generating sample booking data...")
        
        # Set seed for reproducibility
        np.random.seed(42)
        random.seed(42)
        
        # Configuration
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
        
        # Helper functions
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
        
        # Generate base bookings (ground truth)
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
        
        # Generate GA4 events (with realistic issues)
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
        
        # Generate Salesforce opportunities (with realistic issues)
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
        
        # Save to CSV
        ga4_df.to_csv('/opt/airflow/data/raw_ga4_events.csv', index=False)
        sf_df.to_csv('/opt/airflow/data/raw_salesforce_opportunities.csv', index=False)
        base_df.to_csv('/opt/airflow/data/base_truth_bookings.csv', index=False)
        
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
        
        return {
            'base_bookings': len(base_df),
            'ga4_events': len(ga4_df),
            'sf_opportunities': len(sf_df)
        }

    # ========================================================================
    # TASK 1: EXTRACT - Load GA4 Events
    # ========================================================================

    @task(task_id='extract_ga4_events')
    def extract_ga4_events():
        """
        Extract GA4 purchase events and flatten event_params array
        Simulates: SELECT * FROM `project.analytics.events_*` WHERE event_name = 'purchase'
        """
        print("📥 Extracting GA4 events...")
        
        # Read raw GA4 data
        ga4_df = pd.read_csv('/opt/airflow/data/raw_ga4_events.csv')
        
        # Flatten event_params (nested JSON structure)
        flattened_events = []
        
        for _, row in ga4_df.iterrows():
            event_params = eval(row['event_params'])  # In production, use json.loads
            
            # Extract key parameters
            params_dict = {}
            for param in event_params:
                key = param['key']
                if 'string_value' in param['value']:
                    params_dict[key] = param['value']['string_value']
                elif 'double_value' in param['value']:
                    params_dict[key] = param['value']['double_value']
            
            flattened_events.append({
                'event_date': row['event_date'],
                'event_timestamp': row['event_timestamp'],
                'event_name': row['event_name'],
                'user_pseudo_id': row['user_pseudo_id'],
                'transaction_id': params_dict.get('transaction_id'),
                'booking_value': params_dict.get('value'),
                'gclid': params_dict.get('gclid'),
                'utm_source': params_dict.get('utm_source'),
                'utm_campaign': params_dict.get('utm_campaign')
            })
        
        # Save flattened data
        flattened_df = pd.DataFrame(flattened_events)
        
        # Create DuckDB database and load data
        conn = duckdb.connect('/opt/airflow/data/bookings.duckdb')
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("DROP TABLE IF EXISTS staging.ga4_events")
        conn.execute("""
            CREATE TABLE staging.ga4_events AS 
            SELECT * FROM flattened_df
        """)
        
        record_count = conn.execute("SELECT COUNT(*) FROM staging.ga4_events").fetchone()[0]
        
        print(f"✅ Loaded {record_count} GA4 events to staging.ga4_events")
        
        conn.close()
        return record_count

    # ========================================================================
    # TASK 2: EXTRACT - Load Salesforce Opportunities
    # ========================================================================

    @task(task_id='extract_salesforce_opportunities')
    def extract_salesforce_opportunities():
        """
        Extract Salesforce opportunities
        Simulates: SELECT * FROM salesforce.opportunities WHERE stage_name != 'Cancelled'
        """
        print("📥 Extracting Salesforce opportunities...")
        
        # Read raw Salesforce data
        sf_df = pd.read_csv('/opt/airflow/data/raw_salesforce_opportunities.csv')
        
        # Convert date strings to datetime
        sf_df['created_date'] = pd.to_datetime(sf_df['created_date'])
        sf_df['check_in_date'] = pd.to_datetime(sf_df['check_in_date'])
        sf_df['check_out_date'] = pd.to_datetime(sf_df['check_out_date'])
        
        # Create DuckDB database and load data
        conn = duckdb.connect('/opt/airflow/data/bookings.duckdb')
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("DROP TABLE IF EXISTS staging.salesforce_opportunities")
        conn.execute("""
            CREATE TABLE staging.salesforce_opportunities AS 
            SELECT * FROM sf_df
        """)
        
        record_count = conn.execute("SELECT COUNT(*) FROM staging.salesforce_opportunities").fetchone()[0]
        
        print(f"✅ Loaded {record_count} Salesforce opportunities to staging.salesforce_opportunities")
        
        conn.close()
        return record_count

    # ========================================================================
    # TASK 3: TRANSFORM - Data Quality Checks
    # ========================================================================

    @task(task_id='data_quality_checks')
    def data_quality_checks(ga4_count: int, sf_count: int):
        """
        Run data quality checks before reconciliation
        - Check for null transaction IDs
        - Check for duplicate bookings
        - Validate date ranges
        """
        print("🔍 Running data quality checks...")
        
        conn = duckdb.connect('/opt/airflow/data/bookings.duckdb')
        
        # Check 1: Null transaction IDs
        ga4_nulls = conn.execute("""
            SELECT COUNT(*) FROM staging.ga4_events 
            WHERE transaction_id IS NULL
        """).fetchone()[0]
        
        sf_nulls = conn.execute("""
            SELECT COUNT(*) FROM staging.salesforce_opportunities 
            WHERE confirmation_number IS NULL
        """).fetchone()[0]
        
        print(f"   GA4 null transaction_ids: {ga4_nulls}")
        print(f"   Salesforce null confirmation_numbers: {sf_nulls}")
        
        # Check 2: Duplicates
        ga4_dupes = conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT transaction_id) 
            FROM staging.ga4_events
            WHERE transaction_id IS NOT NULL
        """).fetchone()[0]
        
        print(f"   GA4 duplicate transactions: {ga4_dupes}")
        
        # Check 3: Test bookings (common issue)
        test_bookings = conn.execute("""
            SELECT COUNT(*) FROM staging.ga4_events 
            WHERE user_pseudo_id = 'test_user_internal'
        """).fetchone()[0]
        
        print(f"   Test bookings in GA4: {test_bookings}")
        
        conn.close()
        
        print("✅ Data quality checks complete")
        
        return {
            'ga4_nulls': ga4_nulls,
            'sf_nulls': sf_nulls,
            'test_bookings': test_bookings
        }

    # ========================================================================
    # TASK 4: TRANSFORM - Build Reconciliation Model
    # ========================================================================

    @task(task_id='build_reconciliation_model')
    def build_reconciliation_model():
        """
        Core reconciliation logic - matches GA4 events to Salesforce opportunities
        This is the MONEY QUERY for the interview
        """
        print("🔄 Building reconciliation model...")
        
        conn = duckdb.connect('/opt/airflow/data/bookings.duckdb')
        conn.execute("CREATE SCHEMA IF NOT EXISTS reporting")
        
        # The reconciliation query (THIS IS WHAT YOU'LL WRITE IN THE INTERVIEW)
        reconciliation_query = """
        CREATE OR REPLACE TABLE reporting.booking_reconciliation AS
        WITH ga4_bookings AS (
            SELECT 
                STRPTIME(CAST(event_date AS VARCHAR), '%Y%m%d') as booking_date,
                to_timestamp(event_timestamp / 1000000.0) as booking_timestamp,
                user_pseudo_id,
                transaction_id,
                booking_value,
                gclid,
                utm_source,
                utm_campaign,
                -- Flag test bookings
                CASE 
                    WHEN user_pseudo_id = 'test_user_internal' THEN true 
                    ELSE false 
                END as is_test_booking
            FROM staging.ga4_events
            WHERE transaction_id IS NOT NULL
        ),
        
        sf_opportunities AS (
            SELECT
                CAST(created_date AS DATE) as opportunity_date,
                created_date as opportunity_timestamp,
                confirmation_number as transaction_id,
                amount as booking_value,
                property_name,
                stage_name,
                booking_source,
                gclid__c as gclid,
                utm_source__c as utm_source,
                check_in_date,
                nights
            FROM staging.salesforce_opportunities
            WHERE stage_name != 'Cancelled'
        )
        
        SELECT
            COALESCE(g.booking_date, s.opportunity_date) as date,
            COALESCE(g.transaction_id, s.transaction_id) as transaction_id,
            
            -- Match status
            CASE
                WHEN g.transaction_id IS NOT NULL AND s.transaction_id IS NOT NULL THEN 'matched'
                WHEN g.transaction_id IS NOT NULL AND s.transaction_id IS NULL THEN 'ga4_only'
                WHEN g.transaction_id IS NULL AND s.transaction_id IS NOT NULL THEN 'sf_only'
            END as match_status,
            
            -- GA4 data
            g.booking_value as ga4_booking_value,
            g.gclid as ga4_gclid,
            g.utm_source as ga4_utm_source,
            g.is_test_booking,
            
            -- Salesforce data
            s.booking_value as sf_booking_value,
            s.property_name,
            s.stage_name,
            s.booking_source,
            s.gclid as sf_gclid,
            s.check_in_date,
            s.nights,
            
            -- GCLID match status (critical for attribution)
            CASE
                WHEN g.gclid IS NOT NULL AND s.gclid IS NOT NULL AND g.gclid = s.gclid THEN 'gclid_matched'
                WHEN g.gclid IS NOT NULL AND s.gclid IS NULL THEN 'gclid_in_ga4_only'
                WHEN g.gclid IS NULL AND s.gclid IS NOT NULL THEN 'gclid_in_sf_only'
                WHEN g.gclid IS NOT NULL AND s.gclid IS NOT NULL AND g.gclid != s.gclid THEN 'gclid_mismatch'
                ELSE 'no_gclid'
            END as gclid_status,
            
            -- Timing difference (for investigation)
            CASE 
                WHEN g.booking_timestamp IS NOT NULL AND s.opportunity_timestamp IS NOT NULL 
                THEN DATEDIFF('minute', CAST(g.booking_timestamp AS TIMESTAMP), CAST(s.opportunity_timestamp AS TIMESTAMP))
            END as time_diff_minutes
            
        FROM ga4_bookings g
        FULL OUTER JOIN sf_opportunities s
            ON g.transaction_id = s.transaction_id
        """
        
        conn.execute(reconciliation_query)
        
        total_records = conn.execute("SELECT COUNT(*) FROM reporting.booking_reconciliation").fetchone()[0]
        
        print(f"✅ Reconciliation model built with {total_records} records")
        
        conn.close()
        return total_records

    # ========================================================================
    # TASK 5: ANALYZE - Generate Reconciliation Report
    # ========================================================================

    @task(task_id='generate_reconciliation_report')
    def generate_reconciliation_report():
        """
        Generate summary statistics and identify issues
        This is what you'd present to stakeholders
        """
        print("📊 Generating reconciliation report...")
        
        conn = duckdb.connect('/opt/airflow/data/bookings.duckdb')
        
        # Overall match rate
        print("\n" + "="*80)
        print("BOOKING RECONCILIATION SUMMARY")
        print("="*80)
        
        match_summary = conn.execute("""
            SELECT 
                match_status,
                COUNT(*) as record_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM reporting.booking_reconciliation
            GROUP BY match_status
            ORDER BY record_count DESC
        """).fetchdf()
        
        print("\n📈 Match Status:")
        print(match_summary.to_string(index=False))
        
        # GCLID analysis (critical for attribution)
        print("\n🔗 GCLID Attribution Analysis:")
        gclid_summary = conn.execute("""
            SELECT 
                gclid_status,
                COUNT(*) as record_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM reporting.booking_reconciliation
            WHERE match_status = 'matched'
            GROUP BY gclid_status
            ORDER BY record_count DESC
        """).fetchdf()
        
        print(gclid_summary.to_string(index=False))
        
        # Discrepancy breakdown
        print("\n🔍 GA4-Only Bookings (Why no Salesforce match?):")
        ga4_only_analysis = conn.execute("""
            SELECT 
                CASE 
                    WHEN is_test_booking THEN 'Test Booking'
                    WHEN ga4_booking_value < 100 THEN 'Low Value (Possible Fraud)'
                    ELSE 'Unknown - Needs Investigation'
                END as likely_reason,
                COUNT(*) as count
            FROM reporting.booking_reconciliation
            WHERE match_status = 'ga4_only'
            GROUP BY 1
            ORDER BY count DESC
        """).fetchdf()
        
        print(ga4_only_analysis.to_string(index=False))
        
        print("\n🔍 Salesforce-Only Opportunities (Why no GA4 event?):")
        sf_only_analysis = conn.execute("""
            SELECT 
                booking_source,
                COUNT(*) as count
            FROM reporting.booking_reconciliation
            WHERE match_status = 'sf_only'
            GROUP BY booking_source
            ORDER BY count DESC
        """).fetchdf()
        
        print(sf_only_analysis.to_string(index=False))
        
        # Daily trend
        print("\n📅 Daily Match Rate Trend:")
        daily_trend = conn.execute("""
            SELECT 
                date,
                COUNT(*) as total_bookings,
                SUM(CASE WHEN match_status = 'matched' THEN 1 ELSE 0 END) as matched,
                ROUND(SUM(CASE WHEN match_status = 'matched' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as match_rate_pct
            FROM reporting.booking_reconciliation
            GROUP BY date
            ORDER BY date DESC
            LIMIT 10
        """).fetchdf()
        
        print(daily_trend.to_string(index=False))
        
        # Revenue reconciliation
        print("\n💰 Revenue Reconciliation:")
        revenue_summary = conn.execute("""
            SELECT 
                SUM(ga4_booking_value) as total_ga4_revenue,
                SUM(sf_booking_value) as total_sf_revenue,
                SUM(ga4_booking_value) - SUM(sf_booking_value) as revenue_difference,
                ROUND((SUM(ga4_booking_value) - SUM(sf_booking_value)) / SUM(ga4_booking_value) * 100, 2) as diff_percentage
            FROM reporting.booking_reconciliation
            WHERE match_status = 'matched'
        """).fetchdf()
        
        print(revenue_summary.to_string(index=False))
        
        print("\n" + "="*80)
        print("✅ Report generation complete")
        print("="*80)
        
        conn.close()
        
        return "Report generated successfully"

    # ========================================================================
    # DAG STRUCTURE
    # ========================================================================

    # Generate data (NEW - FIRST TASK)
    generate_task = generate_sample_data()

    # Extract tasks (wait for data generation)
    ga4_task = extract_ga4_events()
    sf_task = extract_salesforce_opportunities()

    # Quality checks (waits for both extracts)
    quality_task = data_quality_checks(ga4_task, sf_task)

    # Reconciliation (waits for quality checks)
    reconcile_task = build_reconciliation_model()

    # Report generation (final step)
    report_task = generate_reconciliation_report()

    # Define dependencies
    generate_task >> [ga4_task, sf_task] >> quality_task >> reconcile_task >> report_task