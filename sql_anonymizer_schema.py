import sqlite3
import json
import argparse
import os
from typing import Dict, Any, List, Tuple
import sqlglot

# Add imports for database connectors
try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 library is not installed. Needed for PostgreSQL.")
    print("Please install it using: pip install psycopg2-binary")
    # We won't exit here, allowing SQLite/MySQL to still work if installed

try:
    import mysql.connector
except ImportError:
    print("Error: mysql-connector-python library is not installed. Needed for MySQL.")
    print("Please install it using: pip install mysql-connector-python")
    # We won't exit here, allowing SQLite/PostgreSQL to still work if installed

def get_table_list(cursor: sqlite3.Cursor) -> List[str]:
    """Gets a list of all user tables in the SQLite database."""
    # Select table names from sqlite_master, excluding internal tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_table_schema(cursor: sqlite3.Cursor, table_name: str) -> List[Tuple]:
    """Gets the schema of a table using PRAGMA table_info()."""
    # PRAGMA table_info(table_name) returns:
    # (cid, name, type, notnull, dflt_value, pk)
    # Sanitize table name to prevent injection and handle quotes
    escaped_table_name = table_name.replace('"', '""')
    cursor.execute(f'PRAGMA table_info("{escaped_table_name}");')
    return cursor.fetchall()

def build_create_table_sql(anonymized_table_name: str, column_schemas: List[Tuple], column_mapping: Dict[str, str]) -> str:
    """Builds the CREATE TABLE SQL statement with anonymized names."""
    columns_sql_parts = []
    primary_keys = []
    # Sanitize anonymized table name
    escaped_anonymized_table_name = anonymized_table_name.replace('"', '""')
    for cid, name, type, notnull, dflt_value, pk in column_schemas:
        anonymized_col_name = column_mapping[name]
        # Sanitize column names just in case, though the anonymization scheme prevents this
        escaped_anonymized_col_name = anonymized_col_name.replace('"', '""')
        col_def = f'"{escaped_anonymized_col_name}" {type}' # Quote names to handle potential reserved keywords

        if notnull:
            col_def += " NOT NULL"
        if dflt_value is not None:
            # Need to handle dflt_value carefully, especially if it's a string literal
            # sqlite_master does not give quoting info, so trust PRAGMA's value format
            # Or, check the type. If type is TEXT/VARCHAR and value is quoted, keep quotes.
            # Simplest is to use the provided dflt_value string directly.
            # dflt_value is already a string representation, potentially quoted for strings.
            col_def += f" DEFAULT {dflt_value}"

        if pk:
            primary_keys.append(f'"{escaped_anonymized_col_name}"') # Quote PK names too

        columns_sql_parts.append(col_def)

    create_table_sql = f'CREATE TABLE "{escaped_anonymized_table_name}" (\n    ' + ",\n    ".join(columns_sql_parts)

    # Add primary key constraint if any
    if primary_keys:
        create_table_sql += f",\n    PRIMARY KEY ({', '.join(primary_keys)})"

    create_table_sql += "\n);"
    return create_table_sql

def copy_table_data(src_cursor: sqlite3.Cursor, dest_cursor: sqlite3.Cursor,
                    original_table_name: str, anonymized_table_name: str,
                    original_column_names: List[str], anonymized_column_names: List[str]):
    """Copies data from the original table to the anonymized table."""
    if not original_column_names:
        print(f"Warning: No columns found for table '{original_table_name}'. Skipping data copy.")
        return

    # Sanitize table names
    escaped_original_table_name = original_table_name.replace('"', '""')
    escaped_anonymized_table_name = anonymized_table_name.replace('"', '""')

    # Sanitize original column names for the SELECT statement
    escaped_original_column_names = [name.replace('"', '""') for name in original_column_names]
    # Sanitize anonymized column names for the INSERT statement
    escaped_anonymized_column_names_list = [name.replace('"', '""') for name in anonymized_column_names]

    # Select all data from the original table
    # Select using original column names to ensure order matches the INSERT statement construction
    quoted_columns = [f'"{name}"' for name in escaped_original_column_names]
    select_sql = f'SELECT {", ".join(quoted_columns)} FROM "{escaped_original_table_name}";'
    src_cursor.execute(select_sql)
    data_rows = src_cursor.fetchall()

    if not data_rows:
        print(f"Info: Table '{original_table_name}' is empty. No data to copy.")
        return

    # Prepare the INSERT statement
    # Use anonymized column names in the INSERT INTO part
    # Use placeholders (?) for values
    anonymized_cols_quoted = [f'"{name}"' for name in escaped_anonymized_column_names_list]
    placeholders = ', '.join(['?'] * len(escaped_anonymized_column_names_list))
    insert_sql = f'INSERT INTO "{escaped_anonymized_table_name}" ({", ".join(anonymized_cols_quoted)}) VALUES ({placeholders});'

    # Insert data using executemany for efficiency
    dest_cursor.executemany(insert_sql, data_rows)
    print(f"Copied {len(data_rows)} rows from '{original_table_name}' to '{anonymized_table_name}'.")

def get_db_schema_info(cursor: Any, dbtype: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retrieves schema information (tables and columns) from the connected database.

    Args:
        cursor: The database cursor object.
        dbtype: The type of the database ('sqlite', 'postgres', or 'mysql').

    Returns:
        A dictionary where keys are original table names and values are lists
        of dictionaries representing column information.
    """
    schema_info: Dict[str, List[Dict[str, Any]]] = {}

    if dbtype == 'sqlite':
        tables = get_table_list(cursor)
        for table_name in tables:
            column_schemas = get_table_schema(cursor, table_name)
            schema_info[table_name] = []
            for cid, name, type, notnull, dflt_value, pk in column_schemas:
                schema_info[table_name].append({
                    'name': name,
                    'type': type,
                    'notnull': bool(notnull),
                    'dflt_value': dflt_value,
                    'pk': bool(pk)
                })

    elif dbtype == 'postgres':
        # Query information_schema to get table and column info
        # We need table name, column name, data type, is_nullable, column_default
        cursor.execute("""
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' -- Adjust if using a different schema
            ORDER BY table_name, ordinal_position;
        """)
        for table_name, column_name, data_type, is_nullable, column_default in cursor.fetchall():
            if table_name not in schema_info:
                schema_info[table_name] = []
            schema_info[table_name].append({
                'name': column_name,
                'type': data_type,
                'notnull': is_nullable == 'NO',
                'dflt_value': column_default,
                'pk': False # PK info requires joining with other tables or specific queries
                        # We'll leave this as False for now, can enhance later if needed
            })
        # Note: This doesn't handle composite primary keys or separate PK constraints well.
        # PRAGMA equivalent in postgresql would be querying pg_indexes, pg_constraint, etc.
        # For simplicity, we omit detailed PK info from here for now.

    elif dbtype == 'mysql':
        # Query information_schema to get table and column info
        # Equivalent to postgres, but column_key can help with PK
        cursor.execute("""
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                column_key
            FROM information_schema.columns
            WHERE table_schema = %s -- Use placeholder for database name
            ORDER BY table_name, ordinal_position;
        """, (cursor.connection.database,))
        for table_name, column_name, data_type, is_nullable, column_default, column_key in cursor.fetchall():
             if table_name not in schema_info:
                 schema_info[table_name] = []
             schema_info[table_name].append({
                 'name': column_name,
                 'type': data_type,
                 'notnull': is_nullable == 'NO',
                 'dflt_value': column_default,
                 'pk': column_key == 'PRI' # 'PRI' indicates primary key
             })

    return schema_info

def build_create_table_ast(anonymized_table_name: str, column_schemas: List[Dict[str, Any]], column_mapping: Dict[str, str]) -> sqlglot.expressions.Create:
    """
    Builds a sqlglot AST for a CREATE TABLE statement with anonymized names.

    Args:
        anonymized_table_name: The anonymized name of the table.
        column_schemas: A list of dictionaries containing column information.
        column_mapping: A mapping from original column names to anonymized names.

    Returns:
        A sqlglot Create expression AST node.
    """
    table_identifier = sqlglot.expressions.Identifier(this=anonymized_table_name, quoted=True)
    table = sqlglot.expressions.Table(this=table_identifier)

    definition_expressions = []
    primary_keys = []

    for col in column_schemas:
        original_col_name = col['name']
        anonymized_col_name = column_mapping[original_col_name]
        escaped_anonymized_col_name = anonymized_col_name.replace('"', '""') # Should already be safe due to anonymization scheme, but double check
        col_identifier = sqlglot.expressions.Identifier(this=escaped_anonymized_col_name, quoted=True)

        # Basic data type handling. sqlglot.parse can often handle many types.
        # We'll represent types as Identifier for simplicity.
        # A more sophisticated version might map types explicitly.
        data_type = sqlglot.expressions.DataType(this=sqlglot.expressions.Identifier(this=col['type']))

        column_def = sqlglot.expressions.ColumnDefinition(this=col_identifier, kind=data_type)

        # Add NOT NULL constraint
        if col['notnull']:
            # Create NotNull constraint node
            not_null_constraint = sqlglot.expressions.NotNull()
            # Add constraint to the column definition
            # Note: constraints is a tuple of expressions for ColumnDefinition
            if column_def.constraints:
                 column_def = column_def.copy(constraints=column_def.constraints + (not_null_constraint,))
            else:
                 column_def = column_def.copy(constraints=(not_null_constraint,))


        # Add DEFAULT constraint
        if col['dflt_value'] is not None:
             # sqlglot default handling can be tricky depending on value type (string, number, function)
             # For simplicity, we'll try parsing the default value string. This might fail for complex defaults.
             try:
                 default_expr = sqlglot.parse_one(str(col['dflt_value']), read=None) # Try parsing the default value string
                 if default_expr:
                      # Use the default argument in ColumnDefinition
                      column_def = column_def.copy(default=default_expr)
                 else:
                      print(f"Warning: Could not parse default value '{col['dflt_value']}' for column '{original_col_name}' in table '{anonymized_table_name}'. Skipping default constraint.")
             except sqlglot.errors.ParseError:
                  print(f"Warning: Could not parse default value '{col['dflt_value']}' for column '{original_col_name}' in table '{anonymized_table_name}'. Skipping default constraint.")


        definition_expressions.append(column_def)

        # Collect primary keys for a potential table constraint
        if col['pk']:
            primary_keys.append(col_identifier.copy())

    # Add PRIMARY KEY constraint at the table level if there are primary keys
    if primary_keys:
         # Use sqlglot.expressions.PrimaryKey to represent the constraint
         # This expects a list of column identifiers within a Tuple
         pk_constraint_kind = sqlglot.expressions.PrimaryKey(this=sqlglot.expressions.Tuple(expressions=primary_keys))
         # Wrap the kind in a Constraint expression if needed for table-level constraints
         # sqlglot structure can vary slightly; let's try adding it directly to expressions
         # If sqlglot expects a Constraint() around it, we'll adjust.
         # Based on common AST structures, adding the PrimaryKey directly might work, or it might need to be wrapped.
         # Let's wrap it in a generic Constraint for robustness.
         table_pk_constraint = sqlglot.expressions.Constraint(kind=pk_constraint_kind)
         definition_expressions.append(table_pk_constraint)


    # Create the CREATE TABLE statement AST
    create_table_ast = sqlglot.expressions.Create(
        this=table, # The table expression
        kind='TABLE', # Type of creation
        expressions=definition_expressions # Combined column definitions and table constraints
    )

    return create_table_ast

def anonymize_schema(input_path: str | None = None, output_path: str | None = None, log_path: str | None = None,
                     dbtype: str = 'sqlite', host: str | None = None, database: str | None = None,
                     user: str | None = None, password: str | None = None, port: int | None = None) -> Dict[str, Any]:
    """
    Anonymizes a database schema and data.

    Args:
        input_path: Path to the original SQLite database file (for sqlite dbtype).
        output_path: Path to save the anonymized SQLite database file (for sqlite dbtype) or output directory for SQL scripts.
        log_path: Optional path to save the transformation mapping as a JSON file.
        dbtype: Database type ('sqlite', 'postgres', or 'mysql').
        host: Database host (required for postgres and mysql).
        database: Database name (required for postgres and mysql).
        user: Database user (required for postgres and mysql).
        password: Database password (required for postgres and mysql).
        port: Database port (optional, uses default if not provided).

    Returns:
        A dictionary containing the table and column name mappings.
    """
    table_mapping: Dict[str, str] = {}
    column_mappings: Dict[str, Dict[str, str]] = {} # Nested: {original_table: {original_col: anonymized_col}}
    table_counter = 0
    generated_sql_statements: List[str] = [] # To store generated SQL scripts for non-SQLite

    # Handle output path based on dbtype
    if dbtype == 'sqlite':
        # Ensure the output directory exists for the file
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Remove the output file if it exists to start fresh
        if os.path.exists(output_path):
            os.remove(output_path)
    else:
        # Ensure the output directory exists for SQL scripts
        output_dir = output_path # output_path is the directory for non-sqlite
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        # Note: We don't remove existing scripts here by default.
        # A more robust version might add a flag for this.

    conn_input = None
    cursor_input = None
    conn_output = None # Output connection is only needed for SQLite for now
    cursor_output = None # Output cursor is only needed for SQLite for now

    try:
        # Establish input database connection based on dbtype
        if dbtype == 'sqlite':
            conn_input = sqlite3.connect(input_path)
            conn_input.row_factory = sqlite3.Row # Access columns by name or index
            cursor_input = conn_input.cursor()
            print(f"Connected to SQLite database: {input_path}")

            # Connect to (or create) the output database for SQLite
            conn_output = sqlite3.connect(output_path)
            cursor_output = conn_output.cursor()
            print(f"Opened output SQLite database: {output_path}")

        elif dbtype == 'postgres':
            if psycopg2 is None:
                 raise ImportError("psycopg2 not installed. Please install it using: pip install psycopg2-binary")
            # Build connection string/details
            conn_params = {}
            if host: conn_params['host'] = host
            if database: conn_params['database'] = database
            if user: conn_params['user'] = user
            if password: conn_params['password'] = password
            if port: conn_params['port'] = port
            conn_input = psycopg2.connect(**conn_params)
            cursor_input = conn_input.cursor()
            print(f"Connected to PostgreSQL database: {database or '(default)'} on {host or '(default)'}")
            # No output connection needed here; we will generate SQL scripts

        elif dbtype == 'mysql':
            if mysql.connector is None:
                 raise ImportError("mysql-connector-python not installed. Please install it using: pip install mysql-connector-python")
            # Build connection details dictionary
            conn_config = {}
            if host: conn_config['host'] = host
            if database: conn_config['database'] = database
            if user: conn_config['user'] = user
            if password: conn_config['password'] = password
            if port: conn_config['port'] = port

            conn_input = mysql.connector.connect(**conn_config)
            cursor_input = conn_input.cursor()
            print(f"Connected to MySQL database: {database or '(default)'} on {host or '(default)'}")
            # No output connection needed here; we will generate SQL scripts


        # --- Generalized Schema Extraction ---
        print("Extracting database schema...")
        db_schema = get_db_schema_info(cursor_input, dbtype)
        print(f"Extracted schema for {len(db_schema)} tables.")

        # --- Anonymization and SQL Generation ---
        print("Anonymizing schema and generating output...")
        for original_table_name, column_schemas in db_schema.items():
            table_counter += 1
            anonymized_table_name = f"table_{table_counter}"
            table_mapping[original_table_name] = anonymized_table_name
            column_mappings[original_table_name] = {} # Initialize column mapping for this table

            print(f"Processing table: '{original_table_name}' -> '{anonymized_table_name}'")

            original_column_names = [col['name'] for col in column_schemas]
            anonymized_column_names_list: List[str] = []
            col_counter = 0

            # Build column mapping for this table
            for col in column_schemas:
                original_col_name = col['name']
                col_counter += 1
                anonymized_col_name = f"col_{col_counter}"
                column_mappings[original_table_name][original_col_name] = anonymized_col_name
                anonymized_column_names_list.append(anonymized_col_name)

            # Build sqlglot AST for CREATE TABLE statement
            create_table_ast = build_create_table_ast(
                anonymized_table_name,
                column_schemas,
                column_mappings[original_table_name]
            )

            # Generate CREATE TABLE SQL string based on the target dialect
            create_sql = create_table_ast.sql(dialect=dbtype)

            if dbtype == 'sqlite':
                # For SQLite, execute the generated SQL directly
                cursor_output.execute(create_sql)
                print(f"  Executed CREATE TABLE for '{anonymized_table_name}'.")
            else:
                # For non-SQLite, append the generated SQL to the list
                generated_sql_statements.append(create_sql)
                print(f"  Generated CREATE TABLE statement for '{anonymized_table_name}'.")

            # Copy data from original table to anonymized table
            # This part still needs significant generalization or skipping for non-SQLite.
            if dbtype == 'sqlite':
                 copy_table_data(cursor_input, cursor_output,
                                 original_table_name, anonymized_table_name,
                                 original_column_names, anonymized_column_names_list)
            else:
                 # TODO: Implement generalized data INSERT generation
                 insert_placeholder = f"-- INSERT statements for {anonymized_table_name} (Data copy not yet implemented)"
                 generated_sql_statements.append(insert_placeholder)
                 print(f"  Data copy not yet implemented for dbtype '{dbtype}'.")


        # Commit changes for SQLite output database
        if dbtype == 'sqlite':
             conn_output.commit()

        # Write generated SQL statements to files for non-SQLite databases
        if dbtype != 'sqlite' and output_dir:
            output_sql_path = os.path.join(output_dir, 'anonymized_schema.sql')
            with open(output_sql_path, 'w', encoding='utf-8') as f:
                for stmt in generated_sql_statements:
                    f.write(stmt.strip() + ";\n\n") # Add semicolon and newline
            print(f"Generated SQL schema script: {output_sql_path}")


        print("\nSchema anonymization and data handling complete (schema generation generalized).")

    except (sqlite3.Error, ) as e:
        print(f"SQLite error: {e}")
        # Clean up potentially incomplete output
        if dbtype == 'sqlite' and output_path and os.path.exists(output_path):
            os.remove(output_path)
        return {"error": f"SQLite error: {e}"}
    except (psycopg2.Error if 'psycopg2' in locals() else Exception, ) as e:
        # Catch psycopg2.Error specifically if psycopg2 was imported
        if 'psycopg2' in locals() and isinstance(e, psycopg2.Error):
             print(f"PostgreSQL error: {e}")
             return {"error": f"PostgreSQL error: {e}"}
        else:
            # Re-raise if it's not the expected psycopg2 error or if psycopg2 wasn't imported
            raise e
    except (mysql.connector.Error if 'mysql' in locals() and 'connector' in locals() else Exception,) as e:
        # Catch mysql.connector.Error specifically if mysql.connector was imported
        if 'mysql' in locals() and 'connector' in locals() and isinstance(e, mysql.connector.Error):
             print(f"MySQL error: {e}")
             return {"error": f"MySQL error: {e}"}
        else:
             # Re-raise if it's not the expected mysql.connector error or if it wasn't imported
             raise e
    except ImportError as e:
         print(f"Missing dependency: {e}. Please install the required library.")
         # Clean up potentially incomplete output for SQLite only on ImportError
         if dbtype == 'sqlite' and output_path and os.path.exists(output_path):
             os.remove(output_path)
         return {"error": f"Missing dependency: {e}. Please install the required library."}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Clean up potentially incomplete output for SQLite only on unexpected Exception
        if dbtype == 'sqlite' and output_path and os.path.exists(output_path):
            os.remove(output_path)
        return {"error": f"An unexpected error occurred: {e}"}
    finally:
        # Close input connection
        if conn_input:
            conn_input.close()
        # Close output connection for SQLite
        if conn_output:
            conn_output.close()


    # Combine mappings for output
    full_mapping = {
        "table_mapping": table_mapping,
        "column_mappings_per_table": column_mappings # Use the nested structure
    }

    # Save log to JSON file if log_path is provided
    if log_path:
        try:
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(full_mapping, f, indent=4)
            print(f"Transformation mapping saved to '{log_path}'.")
        except IOError as e:
            print(f"Error saving mapping log to '{log_path}': {e}")

    return full_mapping

# --- CLI Implementation ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anonymize SQL database schema and data.")
    parser.add_argument(
        '--dbtype',
        required=True,
        choices=['sqlite', 'postgres', 'mysql'],
        help='Database type (sqlite, postgres, or mysql)'
    )
    parser.add_argument(
        '--input', '-i',
        help='Path to the input original SQLite database file (required for --dbtype sqlite).'
    )
    parser.add_argument(
        '--output', '-o',
        help='Path to the output anonymized SQLite database file (required for --dbtype sqlite) or output directory for SQL scripts.'
    )
    parser.add_argument(
        '--log', '-l',
        help='Optional path to save the transformation mapping as a JSON file.'
    )
    parser.add_argument(
        '--host',
        help='Database host (required for --dbtype postgres and mysql).'
    )
    parser.add_argument(
        '--database', '-d',
        help='Database name (required for --dbtype postgres and mysql).'
    )
    parser.add_argument(
        '--user', '-u',
        help='Database user (required for --dbtype postgres and mysql).'
    )
    parser.add_argument(
        '--password', '-p',
        help='Database password (required for --dbtype postgres and mysql).'
    )
    parser.add_argument(
        '--port',
        type=int,
        help='Database port (optional, uses default if not provided).')

    args = parser.parse_args()

    # Basic validation based on dbtype
    if args.dbtype == 'sqlite':
        if not args.input or not args.output:
            parser.error("--input and --output are required for --dbtype sqlite")
        if not os.path.exists(args.input):
            print(f"Error: Input file not found at '{args.input}'")
            exit(1)
    elif args.dbtype in ['postgres', 'mysql']:
        if not args.host or not args.database or not args.user or not args.password:
             parser.error("--host, --database, --user, and --password are required for --dbtype {args.dbtype}")

    # Call the anonymization function with parsed arguments
    mapping = anonymize_schema(
        input_path=args.input,
        output_path=args.output,
        log_path=args.log,
        dbtype=args.dbtype,
        host=args.host,
        database=args.database,
        user=args.user,
        password=args.password,
        port=args.port
    )

    # Print mapping to console (unless an error occurred and was reported)
    if "error" not in mapping:
        print("\n--- Transformation Mapping ---")
        print("Table Mapping:")
        for original, anonymized in mapping.get("table_mapping", {}).items():
            print(f"  {original} -> {anonymized}")

        print("\nColumn Mappings (per table):")
        for table, col_map in mapping.get("column_mappings_per_table", {}).items():
             # Find the anonymized table name for printing
             anonymized_table_name = mapping.get("table_mapping", {}).get(table, table) # Use original if anonymized not found
             print(f" Table '{anonymized_table_name}':")
             for original_col, anonymized_col in col_map.items():
                 print(f"    {original_col} -> {anonymized_col}")
        print("----------------------------")
    elif args.log: # If error occurred and log path was given, error is already in log file
         pass # Do nothing here, error was printed during execution and saved to log
    # If error occurred and no log path was given, the error was already printed during execution
