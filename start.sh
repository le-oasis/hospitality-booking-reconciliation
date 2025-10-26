#!/bin/bash

echo "=========================================="
echo "🏨 HOSPITALITY BOOKING RECONCILIATION"
echo "=========================================="
echo ""
echo "Setting up your interview demo project..."
echo ""

# Create required directories
mkdir -p dags logs plugins data dbt

# Set permissions
echo "Setting permissions..."
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Start Airflow
echo ""
echo "🚀 Starting Airflow (this takes ~60 seconds)..."
docker-compose up -d

echo ""
echo "⏳ Waiting for Airflow to initialize..."
sleep 45

echo ""
echo "✅ Airflow is ready!"
echo ""
echo "=========================================="
echo "NEXT STEPS:"
echo "=========================================="
echo ""
echo "1. Generate Data:"
echo "   docker-compose exec airflow-scheduler python /opt/airflow/generate_data.py"
echo ""
echo "2. Access Airflow UI:"
echo "   URL: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "3. Run the Pipeline:"
echo "   Find 'booking_reconciliation_pipeline' DAG"
echo "   Click the play button to trigger it"
echo ""
echo "4. View Results:"
echo "   Click on the last task (generate_reconciliation_report)"
echo "   Check the logs to see the analysis!"
echo ""
echo "=========================================="
