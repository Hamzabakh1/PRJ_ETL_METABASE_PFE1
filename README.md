# ETL Pipeline for Snowflake Data Warehouse (BeeOne Project)

An end‑to‑end Extract‑Transform‑Load (ETL) pipeline that consolidates data from multiple client SQL Server databases into a central Snowflake data warehouse for analysis (e.g., via Metabase BI). Built with modular Prefect flows, pandas transformations, and Snowflake’s optimized bulk‐load tools.

---

## Table of Contents

1. [Features](#features)  
2. [Technologies Used](#technologies-used)  
3. [Installation](#installation)  
4. [Configuration](#configuration)  
5. [Usage](#usage)  
   - [1. Create Snowflake Tables](#1-create-snowflake-tables)  
   - [2. Run the ETL Flow](#2-run-the-etl-flow)  
   - [3. Merge Client Data (Optional)](#3-merge-client-data-optional)  
6. [Project Structure](#project-structure)  
7. [Contributing](#contributing)  
8. [License](#license)  

---

## Features

- **Modular ETL Flow**  
  Separate Prefect [@flow] and [@task] definitions for **Extract → Transform → Load**, improving readability and maintainability.  
- **Multi‑Client Support**  
  Per‑client YAML configuration files allow single‑client or batch (“all”) processing via an interactive CLI (powered by [Questionary](https://github.com/tmbo/questionary)).  
- **Automated Schema Creation**  
  Reads DDL in `Tables/Table/create_tables.sql` to create (or replace) target tables in each client’s Snowflake schema.  
- **Data Cleaning & Transformation**  
  - Drops duplicates & normalizes column names to UPPERCASE  
  - Converts date/time formats into Snowflake‑compatible strings  
- **Efficient Bulk Loading**  
  Uses Snowflake’s [`write_pandas`](https://docs.snowflake.com/en/user-guide/python-connector-pandas.html) for high‑throughput bulk inserts.  
- **Incremental Merge (Planned)**  
  Structure in place for PK‐based MERGE operations; by default runs full loads (truncate + insert).  
- **Centralized Consolidation**  
  `Merge/Merge.py` script iterates all client schemas, appends new records into a global schema (e.g., `BEE_MERGE.PUBLIC`), tagging each row with `ID_CLIENT`.

---

## Technologies Used

| Component                   | Docs & Links                                                                                 |
| --------------------------- | -------------------------------------------------------------------------------------------- |
| **Python 3.9+**             | —                                                                                            |
| **Prefect 2.x**             | [https://docs.prefect.io](https://docs.prefect.io)                                           |
| **Snowflake Connector**     | [https://docs.snowflake.com/](https://docs.snowflake.com/)                                   |
| **pandas & NumPy**          | [https://pandas.pydata.org/](https://pandas.pydata.org/)                                     |
| **SQLAlchemy + PyODBC**     | [https://docs.sqlalchemy.org/](https://docs.sqlalchemy.org/) & [https://github.com/mkleehammer/pyodbc](https://github.com/mkleehammer/pyodbc) |
| **PyYAML**                  | [https://pyyaml.org/](https://pyyaml.org/)                                                   |
| **Questionary (CLI)**       | [https://github.com/tmbo/questionary](https://github.com/tmbo/questionary)                   |
| **Metabase BI (example)**   | [https://www.metabase.com/docs/latest/](https://www.metabase.com/docs/latest/)               |

---

## Installation

1. **Clone the repo**  
   ```bash
   git clone https://github.com/Hamzabakh1/PRJ_ETL_METABASE_PFE1.git
   cd PRJ_ETL_METABASE_PFE1
   ```

2. **Create & activate virtual environment**  
   ```bash
   python3 -m venv venv
   source venv/bin/activate    # Windows: venv\Scripts\activate
   ```

3. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
   pip install pyodbc           # if not included by default
   ```

4. **ODBC Driver (SQL Server)**  
   - **Windows:** Install “ODBC Driver 17 for SQL Server”  
   - **Linux/Mac:** Follow Microsoft’s docs to install `unixODBC` + Microsoft ODBC driver.  
   ([learn.microsoft.com](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server))  

5. **Snowflake account**  
   - Ensure you have a Snowflake account, database, warehouse, and appropriate role permissions.

---

## Configuration

Each client lives in `Clients/<ClientName>/config.yml`:

```yaml
client_id: Client1
client_name: "Client 1"
source_db:
  driver: "{ODBC Driver 17 for SQL Server}"
  server: YOUR_SQL_SERVER
  database: YOUR_DATABASE
  trusted_connection: "yes"     # or "no" + user/password
snowflake:
  account: YOUR_ACCOUNT
  user: YOUR_USER
  password: YOUR_PASSWORD       # or leave blank and use SF_PASSWORD env var
  warehouse: YOUR_WH
  database: YOUR_DB
  schema: CLIENT1
  role: YOUR_ROLE
etl:
  create_or_replace: false      # false = truncate + insert; true = create/replace
  date_format: "%Y-%m-%d"
  etl_flow: "Flows/ETL/flow_prefect.py"
```

> **Tip:** For Merge script, you can also set `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD` as environment variables.

---

## Usage

### 1. Create Snowflake Tables

```bash
python Flows/Creation/creation.py --client Client1
# Options: --replace, --dry-run, --schema OTHER_SCHEMA
```

### 2. Run the ETL Flow

```bash
python Flows/ETL/flow_prefect.py
```

- **Interactive:** Prompts for client name or `all`.  
- **Non‑interactive:** Defaults to first client (or `Client1`).

### 3. Merge Client Data (Optional)

```bash
python Merge/Merge.py
```

- Reads control table `CLIENT_DATABASES` for schema list  
- Creates/merges into `BEE_MERGE.PUBLIC.*` tables  
- Tags records with `ID_CLIENT`

---

## Project Structure

```
PRJ_ETL_METABASE_PFE1/
├── Clients/
│   ├── Client1/
│   │   └── config.yml
│   └── Code.py               # CLI helper for client configs
├── Flows/
│   ├── Creation/
│   │   └── creation.py       # Table DDL executor
│   └── ETL/
│       ├── extract.py        # SQL → pandas DataFrame
│       ├── transform.py      # Cleaning & normalization
│       ├── load.py           # write_pandas to Snowflake
│       └── flow_prefect.py   # Prefect orchestrator
├── Merge/
│   └── Merge.py              # Cross‑client merge logic
├── Tables/
│   ├── Queries/
│   │   └── queries.py        # SQL SELECT definitions
│   └── Table/
│       └── create_tables.sql # DDL for staging tables
├── requirements.txt
└── README.md                 # ← You are here
```

---

## Contributing

1. Fork the repository  
2. Create a feature branch (`git checkout -b feature/XYZ`)  
3. Commit changes (`git commit -m "Add XYZ"`)  
4. Push to your branch (`git push origin feature/XYZ`)  
5. Open a Pull Request for review  

> **Security:** Do **not** commit any real credentials. Use placeholders or environment variables.

---

## License

All rights reserved by the author. No public license granted—please contact the repository owner for reuse or modification permissions.
