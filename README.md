# SQL Anonymizer

A Python-based tool for anonymizing SQL queries and database schemas, helping protect sensitive information while maintaining data structure and relationships.

## Features

- **SQL Query Anonymization**
  - Replaces sensitive identifiers (tables, columns, databases, literals) with generic placeholders
  - Supports **multiple SQL dialects** for accurate parsing and anonymization (`--dialect` option)
  - Improved handling for **unqualified columns**, attempting to resolve them to the correct table scope
  - **Strict mode** (`--strict-mode`) to provide more explicit warnings for ambiguous or unresolvable unqualified columns
  - Preserves query structure and relationships
  - Provides detailed mapping of original to anonymized names

- **Database Schema Anonymization (SQLite, PostgreSQL, MySQL)**
  - Anonymizes table and column names
  - **Connects directly to databases** (SQLite file, PostgreSQL/MySQL via connection details)
  - Extracts schema information in a **dialect-agnostic** way
  - Generates anonymized `CREATE TABLE` statements using `sqlglot` for **target dialect compatibility**
  - Preserves data types and constraints
  - Maintains primary key information where available
  - **Note:** Data copying is currently only implemented for SQLite. For PostgreSQL/MySQL, only the anonymized schema (CREATE TABLE statements) is generated.
  - Generates mapping documentation

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd sql_anonymizer
```

2. Create and activate a virtual environment:
```bash
# Windows
python -m venv sql_venv
.\sql_venv\Scripts\activate

# Linux/Mac
python -m venv sql_venv
source sql_venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Web Interface

1. Start the Streamlit app:
```bash
streamlit run app.py
```

2. Open your browser and navigate to `http://localhost:8501`

3. Use the interface to:
   - Anonymize SQL queries (select dialect, enable strict mode)
   - Upload and anonymize SQLite databases
   - View and download transformation mappings

### Command Line Interface

#### SQL Query Anonymization
Anonymize a SQL file:
```bash
python sql_anonymizer_query.py --input query.sql --output anonymized_query.sql --dialect postgres --explain
```

Anonymize from stdin to stdout:
```bash
cat query.sql | python sql_anonymizer_query.py --dialect mysql --strict-mode
```

#### Database Schema Anonymization

Anonymize a SQLite database file:
```bash
python sql_anonymizer_schema.py --dbtype sqlite --input original.db --output anonymized.db --log mapping.json
```

Anonymize schema from a PostgreSQL database (generates SQL script):
```bash
python sql_anonymizer_schema.py --dbtype postgres --host your_host --database your_db --user your_user --password your_password --port 5432 --output ./anonymized_pg_schema --log pg_mapping.json
```
*(Output path for non-SQLite is a directory to save generated SQL files)*

Anonymize schema from a MySQL database (generates SQL script):
```bash
python sql_anonymizer_schema.py --dbtype mysql --host your_host --database your_db --user your_user --password your_password --port 3306 --output ./anonymized_mysql_schema --log mysql_mapping.json
```
*(Output path for non-MySQL is a directory to save generated SQL files)*


## Supported SQL Dialects

- PostgreSQL
- MySQL
- SQLite
- BigQuery
- Snowflake
- Redshift
- Oracle
- T-SQL
- Spark
- Hive
- Presto
- Drill
- Teradata

*(Note: Dialect support depends on `sqlglot` capabilities)*

## Requirements

- Python 3.8+
- `sqlglot==19.6.0` # Specify version due to API changes
- `streamlit>=1.20.0`
- `sqlite3` (included in Python standard library)
- `psycopg2-binary` # For PostgreSQL schema anonymization
- `mysql-connector-python` # For MySQL schema anonymization

## Project Structure

```
sql_anonymizer/
├── app.py                 # Streamlit web interface
├── sql_anonymizer_query.py    # SQL query anonymization logic
├── sql_anonymizer_schema.py   # Database schema anonymization logic
├── requirements.txt       # Project dependencies
└── README.md             # This file
```

## Error Handling

The tool includes robust error handling for:
- Missing dependencies (`psycopg2-binary`, `mysql-connector-python`)
- `sqlglot` parsing errors (falls back for query anonymization)
- Database connection issues (`sqlite3`, `psycopg2`, `mysql.connector`)
- File I/O operations
- Invalid CLI arguments

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here]

## Support

For issues and feature requests, please use the GitHub issue tracker. 