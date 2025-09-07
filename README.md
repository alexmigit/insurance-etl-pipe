# Data Engineering | Insurance ETL Pipeline

**`insurance-etl-pipe`**
A Pipeline For Extracting, Transforming, and Loading Insurance Data.

---

## 🛠 Quick Setup

### 1. Snowflake Configuration
Run the [`setup_snowflake.sql`](setup_snowflake.sql) script in a **Snowflake Worksheet** to configure your database and schemas.

---

### 2. Create Virtual Environment
Create and activate a Python virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
```

### 3. Install Dependencies
Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 4. Set Environment Variables
Create a .env file in the project root and populate it with your credentials and configuration.

```bash
SNOWFLAKE_USER=''
SNOWFLAKE_PASSWORD=''
SNOWFLAKE_ACCOUNT=''
SNOWFLAKE_DATABASE=''
SNOWFLAKE_SCHEMA=''
SNOWFLAKE_WAREHOUSE=''
SNOWFLAKE_ROLE=''
SLACK_WEBHOOK_URL=''
```

### 5. Run the Pipeline
Execute the main script:

```bash
python main.py
```

### 6. dbt Core Integration
Load environment variables for dbt:

```bash
set -a; source .env; set +a;
```

Then run your dbt commands (e.g., dbt run).