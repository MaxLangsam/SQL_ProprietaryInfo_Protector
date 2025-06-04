# SQL Anonymizer

A comprehensive Python toolkit for safely anonymizing proprietary and sensitive information in SQL databases and queries. This tool provides two main components:

1. **Query Anonymizer**: Anonymizes SQL queries for use in cloud-based LLMs or other contexts where data privacy is crucial.
2. **Schema Anonymizer**: Anonymizes entire SQLite database schemas and data while preserving relationships and structure.

## Features

### Query Anonymization
* **Dialect Support:** Parses and handles various SQL dialects using `sqlglot` (e.g., PostgreSQL, MySQL, SQLite, T-SQL).
* **Comprehensive Replacement:** Anonymizes database, schema, table, and column names.
* **Literal Anonymization:** Replaces string literals, numbers, and email addresses.
* **Consistency:** Maintains consistent renaming for repeated original names/values.
* **Scoped Column Naming:** Refactored logic to provide column names scoped per table alias, falling back to a global scope for unqualified columns.
* **Structure Preservation:** Uses `sqlglot` to parse and rebuild the SQL AST.
* **Keyword Protection:** Preserves SQL reserved keywords and functions.
* **Aliasing & Scopes:** Handles nested scopes and aliases correctly.
* **Transformation Log:** Optional mapping between original values and placeholders.

### Schema Anonymization
* **Complete Database Anonymization:** Anonymizes entire SQLite database schemas.
* **Data Preservation:** Copies and anonymizes all data while maintaining relationships.
* **Table & Column Mapping:** Creates consistent mappings for tables and columns.
* **Primary Key Support:** Preserves primary key constraints and relationships.
* **Logging:** Optional JSON log of all transformations.
* **Error Handling:** Robust error handling with cleanup on failure.

## Installation

1. **Clone the repository:**
```bash
git clone [repository-url]
cd sql_anonymizer
```

2. **Create and activate virtual environment:**
```bash
python -m venv sql_venv
source sql_venv/bin/activate  # On Windows: sql_venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

## Usage

### Query Anonymization (CLI)

```bash
python sql_anonymizer_query.py [input_file] [output_file] [--explain] [--dialect DIALECT]
```

* `input_file`: Optional. Path to the SQL file to anonymize. Reads from stdin if not provided.
* `output_file`: Optional. Path to save the sanitized SQL output. Writes to stdout if not provided.
* `--explain`: Optional flag. If present, prints the transformation mapping of original values to placeholders after the sanitized SQL.
* `--dialect`: Optional. Specify the SQL dialect for parsing (e.g., `postgres`, `mysql`, `sqlite`, `tsql`). Defaults to `standard`.

### Schema Anonymization (CLI)

```bash
python sql_anonymizer_schema.py --input original.db --output anonymized.db [--log mapping.json]
```

* `--input`: Path to the original SQLite database.
* `--output`: Path to save the anonymized database.
* `--log`: Optional path to save the transformation mapping as a JSON file.

### Web Interface

The project includes a web interface for easy interaction:

```bash
python app.py
```

This will start a local web server, typically at `http://localhost:8501`, where you can:
* Paste and anonymize SQL queries.
* **Select the SQL dialect** for query parsing.
* View the transformation logs (including per-table column mappings).
* Upload and anonymize SQLite database schemas.
* Download anonymized queries and databases.

## Example Usage

### Query Anonymization

**Input Query (PostgreSQL example):**
```sql
SELECT u.first_name::text, o.total_spend
FROM prod_db.users u
JOIN prod_db.orders o ON u.user_id = o.user_id
WHERE u.email = 'jane.doe@example.com' AND o.total_spend > 1000::numeric;
```

**Sanitized Output (with scoped naming - exact output may vary slightly):**
```sql
SELECT t1.col_1::text, t2.col_1
FROM database_1.table_1 t1
JOIN database_1.table_2 t2 ON t1.col_2 = t2.col_2
WHERE t1.col_3 = 'email_1' AND t2.col_1 > num_1;
```

*Note: The exact output may vary slightly depending on the `sqlglot` version and parsing nuances, but the anonymization pattern and scoping behavior will be consistent.*

### Schema Anonymization

The schema anonymizer will:
1. Create a new database with anonymized table and column names.
2. Copy all data with consistent anonymization.
3. Preserve all relationships and constraints.
4. Generate a mapping log of all transformations.

## Dependencies

* `sqlglot`: SQL parsing and manipulation for query anonymization.
* `flask`: (Used in `app.py` implicitly via Streamlit) Web interface.
* `sqlite3`: (Built-in Python library) Database operations for schema anonymization.
* `streamlit`: Web application framework.

## Project Structure

## Limitations and Notes

* Complex or vendor-specific SQL syntax might have edge cases depending on the chosen `sqlglot` dialect support.
* The query anonymizer uses per-table scoping for qualified columns based on aliases. Unqualified columns without a clear table context will use a global fallback counter.
* Schema anonymization currently supports SQLite databases only and does not use `sqlglot`.
* The web interface is designed for local use only.

## Error Handling

The tool includes robust error handling for:
- Invalid SQL syntax
- Unsupported SQL dialects
- Database connection issues
- File I/O operations
- Missing dependencies

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
