#!/bin/bash

echo "====================================="
echo "Pipeline started at: $(date)"

# 1. Navigate to your project folder
cd /Users/hp/Development/Data-Analytics-engineering-portfolio/weather-elt-pipeline/

# 2. Activate your virtual environment
source venv/bin/activate

# 3. Run the Python Extraction (10-Day Lookback)
echo "Running Python Extraction..."
python el_weather.py

# 4. Navigate to the dbt folder
cd weather_transform

# 5. Run dbt in Production mode to clean and build the tables
echo "Running dbt transformations..."
dbt build --target prod

echo "Pipeline finished at: $(date)"
echo "====================================="