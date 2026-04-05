# Azure Medallion Data Pipeline

End-to-end data lakehouse pipeline built on Azure, implementing the Medallion Architecture (Bronze → Silver → Gold) for retail transaction data. Orchestrated via Apache Airflow with infrastructure provisioned through Terraform.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│   PostgreSQL (Orders)  │  REST API (Products)  │  CSV (Store Data)  │
└────────────┬───────────┴──────────┬────────────┴────────┬───────────┘
             │                      │                     │
             ▼                      ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                    │
│              Azure Data Factory  +  Python Ingestion Scripts         │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 BRONZE LAYER  (Raw / Immutable)                      │
│         ADLS Gen2  │  Delta Lake format  │  Partitioned by date      │
│         Parquet files  │  Schema-on-read  │  Full history retained   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼  (PySpark on Azure Databricks)
┌─────────────────────────────────────────────────────────────────────┐
│                 SILVER LAYER  (Cleansed / Conformed)                 │
│   Deduplication  │  Null handling  │  Type casting  │  PII masking  │
│   Delta Lake with ACID transactions  │  Schema enforcement           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼  (dbt transformations)
┌─────────────────────────────────────────────────────────────────────┐
│                 GOLD LAYER  (Business / Aggregated)                  │
│   Revenue by region  │  Customer LTV  │  Product performance         │
│   Star schema  │  Optimized for BI  │  Synapse Analytics / Power BI  │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION & MONITORING                         │
│          Apache Airflow (AKS)  │  Azure Monitor  │  Data Quality     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Azure Data Lake Storage Gen2 |
| File Format | Delta Lake (Parquet + transaction log) |
| Compute | Azure Databricks (PySpark) |
| Orchestration | Apache Airflow on AKS |
| Transformation | dbt Core |
| Infrastructure | Terraform |
| Data Factory | Azure Data Factory |
| Serving | Azure Synapse Analytics |
| Monitoring | Azure Monitor + Great Expectations |

---

## Project Structure

```
azure-medallion-pipeline/
├── infra/terraform/             # Azure infrastructure as code
├── ingestion/                   # ADF-triggered ingestion scripts
├── bronze/                      # Raw landing layer
├── silver/                      # PySpark cleansing jobs
├── gold/                        # Aggregated business layer
├── dbt_project/                 # dbt models for gold layer
├── orchestration/               # Airflow DAGs
├── utils/                       # Shared helpers
└── tests/                       # Unit + integration tests
```

---

## Setup

### Prerequisites
- Azure subscription with Contributor access
- Terraform >= 1.5
- Python 3.10+
- Databricks CLI
- dbt-synapse adapter

### 1. Provision Infrastructure
```bash
cd infra/terraform
terraform init
terraform plan -var-file="dev.tfvars"
terraform apply
```

### 2. Configure Environment
```bash
cp .env.example .env
# Fill in ADLS account, Databricks host/token, Synapse connection string
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run Full Pipeline
```bash
# Ingest
python ingestion/ingest_orders.py --env dev --date 2024-01-01

# Bronze → Silver → Gold
python bronze/write_bronze.py --source orders --date 2024-01-01
python silver/clean_orders.py --date 2024-01-01
cd dbt_project && dbt run --select gold && dbt test
```

### 5. Trigger via Airflow
```bash
airflow dags trigger medallion_pipeline --conf '{"date": "2024-01-01"}'
```

---

## Data Quality

Great Expectations validates data at the Silver layer:
- No null `order_id` or `customer_id`
- `order_amount` > 0
- `order_date` within expected range
- Referential integrity between orders and products

```bash
python silver/data_quality.py --layer silver --date 2024-01-01
```

---

## Delta Lake Features Used

- **ACID transactions** — safe concurrent writes
- **Time travel** — `SELECT * FROM orders VERSION AS OF 5`
- **Schema enforcement** — rejects non-conforming records
- **Optimize + Z-Order** — pruning on `order_date` and `region`

---

## dbt Gold Models

| Model | Type | Description |
|---|---|---|
| `fct_revenue` | Fact | Daily revenue by store and product |
| `fct_customer_ltv` | Fact | Lifetime value per customer |
| `dim_store` | Dimension | Store metadata with region mapping |
| `dim_product` | Dimension | Product catalog with category hierarchy |

---

## Key Design Decisions

**Delta Lake over plain Parquet** — ACID guarantees matter when multiple pipelines write concurrently. Delta's transaction log prevents corrupt partial writes and enables reliable upserts via `MERGE`.

**dbt for Gold layer** — Pure SQL aggregation fits dbt's model: version-controlled, tested, documented, with lineage built in. PySpark overhead is unnecessary at this stage.

**Airflow over ADF for orchestration** — ADF handles ingestion triggers well but lacks Python-native branching logic and local testability. Airflow DAGs are code — reviewable and version-controlled.

---

## License

MIT
