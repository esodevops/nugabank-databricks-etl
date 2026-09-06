# NugaBank ETL Pipeline

A production-ready ETL pipeline built on Databricks for processing and analyzing bank transaction data. This project demonstrates best practices for data engineering on Databricks, including data cleaning, transformation, dimensional modeling, and secure storage in Unity Catalog.

## 📊 Project Overview

This ETL pipeline processes raw bank transaction data into a star schema with fact and dimension tables optimized for analytics. The pipeline handles:

- **1M+ transactions** across multiple transaction types (Deposits, Withdrawals, Transfers)
- **Data quality** - null handling, deduplication, and validation
- **Security** - PII masking for credit card and IBAN numbers
- **Dimensional modeling** - Star schema with fact and dimension tables
- **Unity Catalog storage** - ACID-compliant Delta Lake tables

## 🏗️ Architecture

```
Raw CSV Data
    ↓
Spark DataFrame (Data Cleaning)
    ↓
Transformations & Data Quality
    ↓
Dimensional Modeling (Star Schema)
    ├── Transaction Dimension
    ├── Customer Dimension
    ├── Employee Dimension
    └── Fact Table
    ↓
Unity Catalog (Delta Lake)
    ↓
SQL Analytics & BI Tools
```

## 📁 Data Model

### Star Schema Design

**Fact Table**: `workspace.nugabank.fact_table`
- Links all dimensions
- Contains transaction-level details
- Masked sensitive data (PII)
- Foreign keys: `transaction_id`, `customer_id`, `employee_id`

**Dimension Tables**:

1. **Transaction** (`workspace.nugabank.transaction`)
   - Transaction metadata
   - Date, amount, type
   - Primary key: `transaction_id`

2. **Customer** (`workspace.nugabank.customer`)
   - Customer information
   - Name, address, contact details
   - Primary key: `customer_id`

3. **Employee** (`workspace.nugabank.employee`)
   - Employee demographics
   - Company, job title, gender, marital status
   - Primary key: `employee_id`

## 🚀 Getting Started

### Prerequisites

- Databricks workspace (AWS/Azure/GCP)
- Serverless or cluster compute
- Python 3.x
- Access to Unity Catalog

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/esodevops/nugabank-databricks-etl.git
   cd nugabank-databricks-etl
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials (if using external DB)
   ```

3. **Upload to Databricks**
   - Import `nugabank.ipynb.ipynb` into your Databricks workspace
   - Upload the source CSV to Unity Catalog Volume:
     `/Volumes/workspace/default/nugabank/nuga_bank_transactions.csv`

4. **Install dependencies**
   ```python
   %pip install SQLAlchemy psycopg2-binary python-dotenv
   dbutils.library.restartPython()
   ```

5. **Run the pipeline**
   - Execute cells sequentially in the notebook
   - Or run the entire notebook via Databricks Jobs

## 📦 Project Structure

```
databricks-etl/
├── nugabank.ipynb.ipynb      # Main ETL pipeline notebook
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore rules (protects secrets)
├── README.md                 # This file
└── postgresql-42.7.13.jar    # PostgreSQL JDBC driver (local only)
```

## 🔧 Technologies Used

- **Apache Spark** - Distributed data processing
- **PySpark** - Python API for Spark
- **Delta Lake** - ACID transactions and versioning
- **Unity Catalog** - Data governance and cataloging
- **Python 3.x** - Primary programming language
- **SQL** - Data querying and analysis

## 🔐 Security Features

### PII Data Masking
Sensitive information is automatically masked:
- **Credit Card Numbers**: `4532********6789` (last 4 digits visible)
- **IBAN Numbers**: `********1234` (last 4 digits visible)

### Environment Variables
- Credentials stored in `.env` (never committed to Git)
- `.gitignore` prevents accidental secret exposure
- `.env.example` provides template for configuration

## 📊 Data Pipeline Steps

### 1. Data Ingestion
```python
# Read raw CSV data
nugabank_df = spark.read.csv("/Volumes/workspace/default/nugabank/nuga_bank_transactions.csv", 
                              header=True, inferSchema=True)
```

### 2. Data Cleaning
- Convert column names to lowercase
- Fill missing values with 'unknown' or 0
- Drop rows with null `last_updated` timestamps

### 3. Dimensional Modeling
- Extract dimension tables (customer, employee, transaction)
- Generate surrogate keys using `monotonically_increasing_id()`
- Create fact table with foreign key relationships

### 4. Data Security
- Mask PII fields (credit cards, IBANs)
- Keep last 4 digits for reference

### 5. Data Storage
```python
# Save to Unity Catalog Delta tables
transaction.write.mode("overwrite").saveAsTable("workspace.nugabank.transaction")
customer.write.mode("overwrite").saveAsTable("workspace.nugabank.customer")
employee.write.mode("overwrite").saveAsTable("workspace.nugabank.employee")
fact_table.write.mode("overwrite").saveAsTable("workspace.nugabank.fact_table")
```

## 📈 Sample Queries

### Transaction Statistics
```sql
SELECT
    transaction_type,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS average_amount
FROM workspace.nugabank.transaction
GROUP BY transaction_type
ORDER BY total_amount DESC;
```

### Recent Transactions with Customer Details
```sql
SELECT
    t.transaction_id,
    t.transaction_date,
    t.transaction_type,
    t.amount,
    c.customer_name,
    c.customer_city,
    c.customer_country
FROM workspace.nugabank.transaction AS t
INNER JOIN workspace.nugabank.fact_table AS f ON t.transaction_id = f.transaction_id
INNER JOIN workspace.nugabank.customer AS c ON f.customer_id = c.customer_id
ORDER BY t.transaction_date DESC
LIMIT 20;
```

## 🎯 Key Features

✅ **Scalable** - Processes millions of records using Spark
✅ **ACID Compliant** - Delta Lake ensures data integrity
✅ **Version Control** - Delta Lake time travel for auditing
✅ **Data Governance** - Unity Catalog for access control
✅ **PII Protection** - Automated masking of sensitive data
✅ **Dimensional Model** - Optimized star schema for analytics
✅ **Reproducible** - End-to-end automated pipeline

## 📝 Data Quality Checks

The pipeline includes built-in data quality validations:

- **Null Handling**: Missing values filled with appropriate defaults
- **Deduplication**: Dimension tables use `.distinct()` to remove duplicates
- **Type Safety**: Schema inference with validation
- **Referential Integrity**: Foreign key relationships via joins

## 🔄 Unity Catalog Integration

### Why Unity Catalog?

**Unity Catalog tables are NOT stored in Git** because they are:

1. **Data Assets, Not Code** - Tables contain actual data, not code definitions
2. **Stored in Delta Lake** - Physical storage in cloud object storage (S3/ADLS/GCS)
3. **Managed by Databricks** - Metadata catalog separate from Git
4. **Too Large for Git** - Production datasets can be terabytes in size
5. **Governed Separately** - Access control via Unity Catalog, not Git permissions

### What IS in Git:
- ✅ Notebook code (ETL logic)
- ✅ Configuration templates
- ✅ Documentation
- ✅ Scripts and utilities

### Accessing the Data

To access Unity Catalog tables in your Databricks workspace:

```sql
-- List all tables
SHOW TABLES IN workspace.nugabank;

-- Query tables directly
SELECT * FROM workspace.nugabank.transaction LIMIT 10;
```

Or via Python:
```python
# Read from Unity Catalog
df = spark.table("workspace.nugabank.transaction")
df.show()
```

**Commit message prefixes:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `refactor:` - Code restructuring
- `test:` - Adding tests
- `chore:` - Maintenance tasks

### Creating a Pull Request

After pushing to `feature/etl`:

1. Go to GitHub: https://github.com/esodevops/databricks-etl
2. Click **"Compare & pull request"** button
3. Add title: "Add NugaBank ETL Pipeline"
4. Add description explaining your changes
5. Click **"Create pull request"**
6. Wait for review or merge yourself if you have permissions

## 📄 License

This project is licensed under the MIT License.

## 👤 Author

**esodevops**
- GitHub: [@esodevops](https://github.com/esodevops)
- Repository: [databricks-etl](https://github.com/esodevops/databricks-etl)

## 🐛 Issues & Support

Found a bug or have a question? Please open an issue on GitHub.

## 🙏 Acknowledgments

- Databricks for the Unity Catalog platform
- Apache Spark community
- Delta Lake project

---

**Note**: This is a demonstration project. For production use, consider:
- Adding comprehensive error handling
- Implementing incremental loads
- Setting up data quality monitoring
- Adding CI/CD pipelines
- Configuring automated testing