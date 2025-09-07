# Insurance ETL Pipeline

**`insurance-etl-pipe`**
A pipeline for extracting, transforming, and loading insurance data.

---

## 🛠 Setup

### 1. Snowflake Configuration
Run the [`setup_snowflake.sql`](setup_snowflake.sql) script in a **Snowflake Worksheet** to configure your database and schemas.

---

### 2. Virtual Environment
Create and activate a Python virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate    # Windows
```

### 3. Install Dependencies
Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 4. Environment Variables
Create a .env file in the project root and populate it with your credentials and configuration.

### Run the Pipeline

```bash
python main.py
```

### 6. dbt Integration
Load environment variables for dbt:

```bash
set -a; source .env; set +a; dbt run
```

Then run your dbt commands (e.g., dbt run).