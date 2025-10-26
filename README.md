# 🏨 Hospitality Booking Reconciliation Pipeline

**GA4 vs Salesforce Reconciliation | Airflow + DuckDB + dbt**

[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Latest-FFF000?logo=duckdb)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-Ready-FF694B?logo=dbt)](https://www.getdbt.com/)

> A production-grade data pipeline that reconciles Google Analytics 4 (GA4) purchase events with Salesforce booking opportunities, identifying discrepancies and attribution gaps.

---

## 🎯 Problem Statement

**Real-world scenario:** A hospitality client sees a **7-65% discrepancy** between GA4 conversion data and actual Salesforce bookings. They're spending $500K/year on Google Ads but can't properly attribute ROI.

**This pipeline:**
- ✅ Matches GA4 events with Salesforce opportunities by transaction ID
- ✅ Identifies attribution gaps (GCLID tracking issues)
- ✅ Categorizes discrepancies (test bookings, phone reservations, cancellations)
- ✅ Generates executive-ready reconciliation reports

---

## 📊 Demo Results

### Match Rate: **74.81%** 
| Status | Records | % |
|--------|---------|---|
| ✅ Matched | 398 | 74.81% |
| ⚠️ GA4 Only | 90 | 16.92% |
| 🔍 SF Only | 44 | 8.27% |

### GCLID Attribution: **12.06%** (Critical Issue!)
- 🚫 No GCLID: 337 records (84.67%)
- ✅ Matched: 48 records (12.06%)
- ⚠️ Lost in sync: 13 records (3.27%)

### Revenue Accuracy: **99.95%** ✅
- Variance: Only $240 out of $532,322

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- 4GB RAM minimum

### Run in 3 Steps

```bash
# 1. Clone and start
git clone https://github.com/YOUR_USERNAME/hospitality-booking-reconciliation.git
cd hospitality-booking-reconciliation
docker-compose up -d

# 2. Wait 60 seconds, then open Airflow
open http://localhost:8080

# 3. Login (airflow/airflow) and trigger the DAG
```

That's it! The pipeline will:
1. Generate 500 fake bookings with realistic issues
2. Extract to DuckDB staging tables
3. Run data quality checks
4. Build reconciliation model (FULL OUTER JOIN)
5. Generate detailed report

---

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐
│   GA4       │     │ Salesforce  │
│   Events    │     │Opportunities│
└──────┬──────┘     └──────┬──────┘
       │  Airflow Extract  │
       ▼                   ▼
┌──────────────────────────────────┐
│     DuckDB (Staging Layer)       │
│  • staging.ga4_events            │
│  • staging.salesforce_opps       │
└──────────┬───────────────────────┘
           │  Transform (SQL)
           ▼
┌──────────────────────────────────┐
│   Reconciliation Model           │
│  • Match on transaction_id       │
│  • Analyze GCLID attribution     │
│  • Categorize discrepancies      │
└──────────┬───────────────────────┘
           │  Report
           ▼
┌──────────────────────────────────┐
│   Analytics Dashboard            │
│  • Match rate trends             │
│  • GCLID coverage analysis       │
│  • Discrepancy breakdown         │
└──────────────────────────────────┘
```

---

## 🔍 The Money Query

Core reconciliation SQL (this is what you'd write in an interview):

```sql
WITH ga4_bookings AS (
  SELECT 
    transaction_id,
    booking_value,
    gclid,
    CASE WHEN user_pseudo_id = 'test_user_internal' 
         THEN true ELSE false END as is_test_booking
  FROM staging.ga4_events
  WHERE transaction_id IS NOT NULL
),

sf_opportunities AS (
  SELECT
    confirmation_number as transaction_id,
    amount as booking_value,
    gclid__c as gclid,
    booking_source
  FROM staging.salesforce_opportunities
  WHERE stage_name != 'Cancelled'
)

SELECT
  COALESCE(g.transaction_id, s.transaction_id) as transaction_id,
  
  CASE
    WHEN g.transaction_id IS NOT NULL 
     AND s.transaction_id IS NOT NULL THEN 'matched'
    WHEN g.transaction_id IS NOT NULL THEN 'ga4_only'
    ELSE 'sf_only'
  END as match_status,
  
  CASE
    WHEN g.gclid = s.gclid THEN 'gclid_matched'
    WHEN g.gclid IS NOT NULL 
     AND s.gclid IS NULL THEN 'gclid_lost_in_sf'
    ELSE 'no_gclid'
  END as gclid_status

FROM ga4_bookings g
FULL OUTER JOIN sf_opportunities s
  ON g.transaction_id = s.transaction_id
```

---

## 📂 Project Structure

```
hospitality_booking_reconciliation/
├── dags/
│   ├── booking_reconciliation_dag.py  # Main Airflow DAG (6 tasks)
│   └── generate_data.py               # Fake data generator
├── dbt/                               # dbt-ready (expand here)
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
├── docker-compose.yml                 # Airflow + Postgres
├── Dockerfile                         # Custom Airflow image
└── requirements.txt                   # Python dependencies
```

---

## 🎓 Skills Demonstrated

✅ **Airflow:** TaskFlow API, dependencies, error handling  
✅ **SQL:** FULL OUTER JOIN, window functions, CTEs, CASE statements  
✅ **Python:** Pandas, nested JSON flattening, data generation  
✅ **Data Quality:** Null checks, deduplication, test data filtering  
✅ **Business Analysis:** Attribution analysis, discrepancy categorization  
✅ **DevOps:** Docker, containerization, environment management

---

## 💡 Real-World Issues Simulated

This pipeline generates realistic data with common problems:

- ❌ **GCLID Loss (10%):** Payment gateway redirects strip parameters
- ❌ **Test Bookings (10):** Internal QA team testing
- ❌ **Phone Reservations (30):** No web tracking possible
- ❌ **Quick Cancellations (50):** Cancelled before Salesforce sync
- ❌ **Sync Delays:** Salesforce records 5-15 min after GA4

---

## 🎯 Interview Talking Points

### "How would you investigate a 30% GA4-Salesforce discrepancy?"

> "I'd build this exact pipeline:
> 1. Extract both sources to a warehouse
> 2. Run data quality checks (nulls, dupes, test data)
> 3. Match on transaction ID with FULL OUTER JOIN
> 4. Categorize unmatched: test bookings, phone reservations, cancellations
> 5. Analyze GCLID separately (payment gateway often strips it)
> 6. Present match rate trends daily"

### "How do you trace GCLID through a pipeline?"

> "Check each stage:
> - GA4: Is it in event_params?
> - Website: Is it in hidden form fields?
> - Payment gateway: Does redirect preserve it?
> - Salesforce: Is gclid__c populated?
> - Build monitoring query showing GCLID population rate"

### "What's a healthy match rate?"

> "85-95% for hospitality, accounting for:
> - Phone/walk-in bookings (SF only)
> - Test bookings (GA4 only)
> - Quick cancellations (GA4 only)
> - Anything below 80% needs investigation"

---

## 📈 Production Roadmap

To make this production-ready:

- [ ] Replace DuckDB with BigQuery/Databricks
- [ ] Add dbt transformations (staging scaffolded)
- [ ] Set up Fivetran for real GA4/SF ingestion
- [ ] Add data quality monitoring (Monte Carlo)
- [ ] Create Looker dashboard
- [ ] Implement Slack alerts (match rate < 80%)
- [ ] Add incremental processing

---

## 🐛 Troubleshooting

**DAG not showing?**
```bash
docker-compose logs airflow-scheduler | grep booking
```

**DuckDB lock error?**
```bash
docker-compose restart
```

**Fresh start:**
```bash
docker-compose down -v
docker-compose up -d
```

---

## 🧹 Cleanup

```bash
# Stop everything
docker-compose down

# Remove all data
docker-compose down -v
rm -rf data/* logs/*
```

---

## 👤 Author

**Mahmud Oyinloye**  
Data & Analytics Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?logo=linkedin)](https://www.linkedin.com/in/mahmud-o)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?logo=gmail)](mailto:oyinn@outlook.com)

---

## 📄 License

MIT License - Free to use for interviews and demos!

---

## ⭐ Show Your Support

If this helped with your interview prep:
- ⭐ Star this repo
- 🔄 Fork it
- 📢 Share with others

---

**Built for Data Engineering Interview Success** 🚀