# What
Insurance ETL Pipeline (insurance-etl-pipe)

# Setup Snowflake
Run setup_snowflake.sql script in Snowflake Worksheet

# Virtual Environment
python -m venv .venv

# Activate venv
source venv/bin/activate

# Install Dependencies
pip install -r requirements.txt

# Create environment file in root
.env

# Run
python main.py

# dbt
set -a; source .env; set +a; dbt run