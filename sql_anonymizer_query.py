import argparse
import re
from typing import Dict, Any, Tuple, List # Import Tuple, List

# Import ParseError first
from sqlglot.errors import ParseError

# Then attempt to import other sqlglot components
try:
    import sqlglot
    from sqlglot.expressions import (
        Expression, Identifier, Table, Column, Literal, Star,
        QualifiedIdentifier, Schema, Database, Alias, Join, From, Select
    )
except ImportError:
    print("Error: sqlglot library is not installed.")
    print("Please install it using: pip install sqlglot")
    raise ImportError("sqlglot is required but not installed")

# --- Anonymization Logic ---

class SQLAnonymizer:
    """
    Anonymizes SQL queries by replacing sensitive identifiers and literals
    with generic placeholders using sqlglot for parsing and traversal.
    Supports dialect-specific parsing and per-table column naming.
    """
    def __init__(self, dialect: str | None = None, strict_mode: bool = False):
        # Store the selected dialect, default to postgres if none specified
        self.dialect = dialect if dialect and dialect in sqlglot.dialects.Dialects else 'postgres'

        # Mappings: {type: {original_value: placeholder}}
        self.mappings: Dict[str, Dict[Any, str]] = {
            'database': {},
            'schema': {},
            'table': {},
            'string': {},
            'number': {},
            'email': {}
        }
        # Counters: {type: count}
        self.counters: Dict[str, int] = {
            'database': 0,
            'schema': 0,
            'table': 0,
            'string': 0,
            'number': 0,
            'email': 0
        }

        # Per-query state for scoped naming
        # Map alias (or original table name if no alias) to original table path (e.g., db.schema.table)
        self._alias_to_table_map: Dict[str, str] = {}
        # Map original table path to its anonymized name (e.g., db.schema.table -> table_1)
        self._original_table_path_to_anonymized_map: Dict[str, str] = {}
        # Per-table column mappings: {original_table_path: {original_col_name: anonymized_col_name}}
        self._per_table_column_maps: Dict[str, Dict[str, str]] = {}
        # Per-table column counters: {original_table_path: count}
        self._per_table_column_counters: Dict[str, int] = {}

         # Global column map as a fallback for unqualified columns not resolvable to a table
        self._global_column_map: Dict[str, str] = {}
        self._global_column_counter: int = 0

        self.strict_mode = strict_mode

    def _reset_state(self):
        """Resets the state for a new query."""
        self.mappings = {
            'database': {},
            'schema': {},
            'table': {},
            'string': {},
            'number': {},
            'email': {}
        }
        self.counters = {
            'database': 0,
            'schema': 0,
            'table': 0,
            'string': 0,
            'number': 0,
            'email': 0
        }
        self._alias_to_table_map = {}
        self._original_table_path_to_anonymized_map = {}
        self._per_table_column_maps = {}
        self._per_table_column_counters = {}
        self._global_column_map = {}
        self._global_column_counter = 0


    def _get_placeholder(self, type: str, original_value: Any) -> str:
        """Gets or creates a placeholder for a given type and original value."""
        # Normalize string values for consistent mapping (case-insensitive for identifiers)
        # For literals like strings and numbers, keep the original case/format for mapping key
        normalized_value = original_value.lower() if type in ['database', 'schema', 'table'] else original_value

        if normalized_value in self.mappings[type]:
            return self.mappings[type][normalized_value]

        self.counters[type] += 1
        placeholder = f"{type}_{self.counters[type]}"
        self.mappings[type][normalized_value] = placeholder
        return placeholder

    def _get_anonymized_column_name(self, original_table_path: str | None, original_col_name: str) -> str:
        """Gets or creates an anonymized column name, scoped per table or globally."""
        normalized_col_name = original_col_name.lower()

        if original_table_path:
            # Scoped naming if table context is available
            if original_table_path not in self._per_table_column_maps:
                self._per_table_column_maps[original_table_path] = {}
                self._per_table_column_counters[original_table_path] = 0

            if normalized_col_name in self._per_table_column_maps[original_table_path]:
                return self._per_table_column_maps[original_table_path][normalized_col_name]
            else:
                self._per_table_column_counters[original_table_path] += 1
                placeholder = f"col_{self._per_table_column_counters[original_table_path]}"
                self._per_table_column_maps[original_table_path][normalized_col_name] = placeholder
                return placeholder
        else:
            # Fallback to global naming for unqualified columns without clear table context
             if normalized_col_name in self._global_column_map:
                 return self._global_column_map[normalized_col_name]
             else:
                 self._global_column_counter += 1
                 placeholder = f"global_col_{self._global_column_counter}"
                 self._global_column_map[normalized_col_name] = placeholder
                 print(f"Warning: Unqualified column '{original_col_name}' anonymized with global scope as '{placeholder}'. Consider qualifying columns.")
                 return placeholder


    def _preprocess_ast(self, expression: Expression):
        """
        Traverses the AST to identify table aliases and original table paths
        and build necessary mappings before the main transformation.
        """
        def find_aliases_and_tables(node):
            if isinstance(node, Table):
                original_table_path_parts = [part.name for part in node.parts if part.name is not None]
                original_table_path = ".".join(original_table_path_parts)

                if original_table_path not in self._original_table_path_to_anonymized_map:
                     self._original_table_path_to_anonymized_map[original_table_path] = self._get_placeholder('table', original_table_path)

                # Determine the alias. If no explicit alias, use the original table name/last part.
                alias_name = node.alias.this.name if node.alias and isinstance(node.alias.this, Identifier) and node.alias.this.name is not None else node.this.name
                if alias_name: # Ensure alias_name is not None or empty
                     self._alias_to_table_map[alias_name.lower()] = original_table_path # Store alias -> original path mapping

            # Recursively visit children
            for child in node.expressions:
                find_aliases_and_tables(child)

        # Start traversal from the root expression
        find_aliases_and_tables(expression)


    def _anonymize_node(self, node: Expression) -> Expression:
        """Recursively traverses and anonymizes nodes in the SQL AST."""

        # Handle Databases, Schemas, Tables
        if isinstance(node, Database):
            original_name_key = node.this.name if isinstance(node.this, Identifier) and node.this.name is not None else str(node)
            placeholder = self._get_placeholder('database', original_name_key)
            if isinstance(node.this, Identifier):
                 return Database(this=Identifier(this=placeholder, quoted=node.this.quoted),
                                 db=node.db.copy() if node.db else None)
            else:
                 print(f"Warning: Unhandled Database node 'this' type: {type(node.this).__name__} for node {node}. Cannot replace precisely. Returning original.")
                 return node.copy()

        elif isinstance(node, Schema):
            original_name_key = node.this.name if isinstance(node.this, Identifier) and node.this.name is not None else str(node)
            placeholder = self._get_placeholder('schema', original_name_key)
            if isinstance(node.this, Identifier):
                return Schema(this=Identifier(this=placeholder, quoted=node.this.quoted),
                            db=node.db.copy() if node.db else None)
            else:
                print(f"Warning: Unhandled Schema node 'this' type: {type(node.this).__name__} for node {node}. Cannot replace precisely. Returning original.")
                return node.copy()

        elif isinstance(node, Table):
            # Table names were handled in the preprocess step to build mappings.
            # Now we replace the table name based on the anonymized map.
            original_table_path_parts = [part.name for part in node.parts if part.name is not None]
            original_table_path = ".".join(original_table_path_parts)
            anonymized_name = self._original_table_path_to_anonymized_map.get(original_table_path, original_table_path)  # Use original as fallback

            processed_node = node.copy()

            # Find the last Identifier node in the qualified path structure
            current = processed_node.this
            parent = None

            while isinstance(current, QualifiedIdentifier):
                parent = current
                current = current.expression

            if isinstance(current, Identifier):
                new_identifier = Identifier(this=anonymized_name, quoted=current.quoted)
                if parent:
                    parent.expression = new_identifier
                else:
                    processed_node.this = new_identifier
                processed_node.alias = node.alias.copy() if node.alias else None
                return processed_node
            elif isinstance(current, Star):
                return processed_node
            else:
                print(f"Warning: Unhandled final part type {type(current).__name__} in Table node {node}. Cannot replace table name precisely. Returning original.")
                return node.copy()

        elif isinstance(node, Column):
            original_column_name = node.name
            if original_column_name is None:
                return node.copy()

            original_table_path = None
            normalized_col_name = original_column_name.lower()

            # 1. Try to determine table context from qualification (alias.column or table.column)
            if isinstance(node.this, Identifier) and node.this.expression is not None and isinstance(node.this.expression, Identifier):
                # This structure is like alias.column or table.column
                qualifier_name = node.this.expression.name.lower() # Get the 'alias' or 'table' part

                # Look up the original table path using the qualifier name from the alias map
                original_table_path = self._alias_to_table_map.get(qualifier_name)

            # 2. If not qualified or not resolved by alias, try to resolve unqualified column
            #    against tables found in the FROM clause.
            if not original_table_path:
                potential_table_paths = []
                # Check if the column name exists in any of the tables identified in the preprocess step
                for table_path_in_from in self._original_table_path_to_anonymized_map.keys():
                     # Check if we have column mappings for this table (populated during traversal)
                     if table_path_in_from in self._per_table_column_maps and normalized_col_name in self._per_table_column_maps[table_path_in_from]:
                        potential_table_paths.append(table_path_in_from)

                if len(potential_table_paths) == 1:
                    # Found in exactly one table in the FROM clause - resolve to this table's scope
                    original_table_path = potential_table_paths[0]
                    # Note: The column mapping should already exist from when this column was processed
                    # in a qualified form, or it will be created in _get_anonymized_column_name.
                elif len(potential_table_paths) > 1:
                    # Found in multiple tables - ambiguous
                    if self.strict_mode:
                        print(f"Warning (Strict Mode): Unqualified column '{original_column_name}' is ambiguous, found in multiple tables: {potential_table_paths}. Using global fallback.")
                    else:
                        print(f"Warning: Unqualified column '{original_column_name}' is ambiguous, found in multiple tables: {potential_table_paths}. Using global fallback.")
                    # original_table_path remains None, will fall back to global
                else:
                    # Not found in any tables from the FROM clause (based on existing mappings)
                    if self.strict_mode:
                        print(f"Warning (Strict Mode): Unqualified column '{original_column_name}' could not be resolved to a table in the FROM clause. Using global fallback.")
                    else:
                         # This case is often handled by the global fallback in _get_anonymized_column_name
                         # but we log here specifically if it wasn't found in any *pre-identified* tables.
                         pass # Let _get_anonymized_column_name handle the global fallback and its warning.


            # Get the anonymized column name based on the determined table path or globally
            # _get_anonymized_column_name handles creation if it doesn't exist and the global fallback
            anonymized_col_name = self._get_anonymized_column_name(original_table_path, original_column_name)

            processed_node = node.copy()

            # Find the last Identifier node to replace its name
            current = processed_node.this
            parent = None
            while isinstance(current, QualifiedIdentifier):
                parent = current
                current = current.expression

            if isinstance(current, Identifier):
                new_identifier = Identifier(this=anonymized_col_name, quoted=current.quoted)
                if parent:
                    parent.expression = new_identifier
                else:
                    processed_node.this = new_identifier
                processed_node.alias = node.alias.copy() if node.alias else None
                return processed_node
            elif isinstance(current, Star):
                return processed_node
            else:
                print(f"Warning: Unhandled final part type {type(current).__name__} in Column node {node}. Cannot replace column name precisely. Returning original.")
                return node.copy()


        elif isinstance(node, Literal):
            original_value = node.this

            if isinstance(original_value, str) and '@' in original_value:
                 placeholder = self._get_placeholder('email', original_value.lower())
                 return Literal(this=placeholder, is_string=True)

            if node.is_string:
                 placeholder = self._get_placeholder('string', original_value)
                 return Literal(this=placeholder, is_string=True)

            if node.is_number:
                 placeholder = self._get_placeholder('number', original_value)
                 return Literal(this=placeholder, is_string=False)

            return node.copy() # Keep other literals as is

        # Preserve ::type casts - sqlglot parses them, ensure they are output correctly.
        # We don't need to explicitly handle them here unless we want to anonymize the types,
        # which is not requested. The default traversal preserves them.

        # Preserve T-SQL control structures - sqlglot might parse some, others might cause errors.
        # If `parse_one` fails, we catch the ParseError in sanitize_sql.
        # If sqlglot parses them, the transformation should traverse them. We only anonymize specific
        # node types (Identifier, Literal etc.). Control structures like BEGIN, END, DECLARE
        # would likely be different node types which we don't modify, thus preserving them.

        # sqlglot.transform automatically recurses into children unless the node type is handled
        # and a new node is returned. So, we don't need explicit recursion here for unhandled types.

        return node.copy() # Return a copy of the node if not explicitly handled above


    def sanitize_sql(self, query: str, explain: bool = False) -> Tuple[str, Dict[str, Any]]:
        """
        Sanitizes a SQL query string and optionally returns the transformation log.

        Args:
            query: The input SQL query string.
            explain: Whether to generate and return the transformation log.

        Returns:
            A tuple containing the sanitized SQL query string and the transformation
            log dictionary (or an empty dictionary if explain is False).
        """
        self._reset_state() # Reset state for each new query

        try:
            # Parse the query using the specified or default dialect
            # sqlglot.parse returns a list of expressions (for multi-statement queries)
            # parse_one is better for single queries or the first statement
            # Use the stored self.dialect
            ast = sqlglot.parse_one(query, dialect=self.dialect)

            if ast:
                # Preprocess the AST to find tables/aliases before transformation
                self._preprocess_ast(ast)

                # Anonymize the AST by transforming nodes
                sanitized_ast = ast.transform(self._anonymize_node)

                # Generate the sanitized SQL string
                sanitized_query_str = sanitized_ast.sql(dialect=self.dialect) # Use dialect for output too
            else:
                sanitized_query_str = "" # Handle case where parsing returns None? (Shouldn't happen with parse_one on valid input)
                print("Warning: Parsing returned no AST.")

        except ParseError as e:
            print(f"Error parsing SQL query (Dialect: {self.dialect}): {e}")
            # Return original query and an error in the mapping
            return query, {"error": f"Parsing failed for dialect {self.dialect}: {e}"}
        except Exception as e:
            print(f"An unexpected error occurred during sanitization: {e}")
            return query, {"error": f"Unexpected error: {e}"}


        transformation_log: Dict[str, Any] = {}
        if explain:
            # Structure the log for clarity
            transformation_log["identifier_mappings"] = {
                "database": self.mappings["database"],
                "schema": self.mappings["schema"],
                "table": self._original_table_path_to_anonymized_map, # Show original full path -> anonymized name
                "global_column_fallback": self._global_column_map # Show global fallback map
            }
            transformation_log["column_mappings_per_table"] = self._per_table_column_maps # Show per-table maps
            transformation_log["literal_mappings"] = {
                "string": self.mappings["string"],
                "number": self.mappings["number"],
                "email": self.mappings["email"]
            }

        return sanitized_query_str, transformation_log

    # Keep the get_transformation_log method for module usage, update its structure
    def get_transformation_log(self) -> Dict[str, Any]:
         """Returns the transformation log from the last sanitized query."""
         transformation_log: Dict[str, Any] = {}
         transformation_log["identifier_mappings"] = {
                 "database": self.mappings["database"],
                 "schema": self.mappings["schema"],
                 "table": self._original_table_path_to_anonymized_map, # Show original full path -> anonymized name
                 "global_column_fallback": self._global_column_map # Show global fallback map
            }
         transformation_log["column_mappings_per_table"] = self._per_table_column_maps # Show per-table maps
         transformation_log["literal_mappings"] = {
                 "string": self.mappings["string"],
                 "number": self.mappings["number"],
                 "email": self.mappings["email"]
            }
         return transformation_log


# --- CLI Implementation ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anonymize SQL queries.")
    parser.add_argument(
        'input_file',
        nargs='?', # Make input_file optional, defaults to stdin
        help='Path to the input SQL file. Reads from stdin if not provided.'
    )
    parser.add_argument(
        'output_file',
        nargs='?', # Make output_file optional, defaults to stdout
        help='Path to save the sanitized SQL output. Writes to stdout if not provided.'
    )
    parser.add_argument(
        '--explain',
        action='store_true', # Store True when flag is present
        help='If present, prints the transformation mapping of original values to placeholders.'
    )
    # Add the --dialect flag
    parser.add_argument(
        '--dialect',
        default='postgres', # Default dialect
        help='Specify the SQL dialect for parsing (e.g., postgres, mysql, sqlite, tsql).'
    )
    # Add the --strict-mode flag
    parser.add_argument(
        '--strict-mode',
        action='store_true', # Store True when flag is present
        help='Enable strict mode for column anonymization. Unqualified columns that cannot be resolved to a single table will be warned more explicitly.'
    )


    args = parser.parse_args()

    input_content = ""
    if args.input_file:
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                input_content = f.read()
        except FileNotFoundError:
            print(f"Error: Input file not found at '{args.input_file}'")
            exit(1)
        except IOError as e:
            print(f"Error reading input file '{args.input_file}': {e}")
            exit(1)
    else:
        # Read from stdin
        import sys
        input_content = sys.stdin.read()

    # Initialize the anonymizer with the specified dialect and strict mode
    anonymizer = SQLAnonymizer(dialect=args.dialect, strict_mode=args.strict_mode)

    # Sanitize the SQL query
    sanitized_sql, mapping_log = anonymizer.sanitize_sql(input_content, explain=args.explain)

    # Determine output destination
    output_destination = None
    if args.output_file:
        try:
            output_destination = open(args.output_file, 'w', encoding='utf-8')
        except IOError as e:
            print(f"Error opening output file '{args.output_file}' for writing: {e}")
            exit(1)
    else:
        output_destination = sys.stdout # Write to stdout

    # Write sanitized SQL
    output_destination.write(sanitized_sql)
    if not args.output_file: # Add newline for stdout clarity if no log follows
         output_destination.write("\n")


    # Write transformation log if requested
    if args.explain:
        import json
        # Add a separator before the log for clarity, especially if writing to file
        if args.output_file:
             output_destination.write("\n--- Transformation Mapping ---\n")
        else:
             print("\n--- Transformation Mapping ---\n") # Print to stderr or stdout? stdout for now.

        # Check if an error occurred during sanitization
        if "error" in mapping_log:
             log_output = json.dumps(mapping_log, indent=4)
        else:
            # Format the log nicely
            log_output_dict: Dict[str, Any] = {}
            if mapping_log.get("identifier_mappings"):
                 log_output_dict["Identifier Mappings"] = mapping_log["identifier_mappings"]
            if mapping_log.get("column_mappings_per_table"):
                 # Reformat column mappings for better readability in CLI output
                 formatted_col_maps: Dict[str, Dict[str, str]] = {}
                 # Show anonymized table name -> original col -> anonymized col
                 for original_table_path, col_map in mapping_log["column_mappings_per_table"].items():
                      anonymized_table_name = mapping_log["identifier_mappings"]["table"].get(original_table_path, original_table_path)
                      formatted_col_maps[anonymized_table_name] = col_map
                 log_output_dict["Column Mappings (per anonymized table)"] = formatted_col_maps

            if mapping_log.get("literal_mappings"):
                 log_output_dict["Literal Mappings"] = mapping_log["literal_mappings"]

            # Include the global column fallback map explicitly if it was used
            if mapping_log["identifier_mappings"].get("global_column_fallback"):
                 log_output_dict["Global Column Fallback Mapping (for unqualified columns)"] = mapping_log["identifier_mappings"]["global_column_fallback"]
                 # Remove from identifier_mappings to avoid duplication
                 del log_output_dict["Identifier Mappings"]["global_column_fallback"]


            log_output = json.dumps(log_output_dict, indent=4)

        output_destination.write(log_output)
        if args.output_file:
            output_destination.write("\n") # Ensure newline at the end of file output


    # Close the output file if it was opened
    if args.output_file and output_destination and output_destination != sys.stdout:
        output_destination.close()
