import streamlit as st
import json
from typing import Dict, Any, Tuple, List
import os
import sqlite3
from sql_anonymizer_query import SQLAnonymizer
from sql_anonymizer_schema import anonymize_sqlite_schema  # Updated import

# --- Streamlit App Configuration ---
st.set_page_config(
    page_title="SQL Anonymizer",
    page_icon="🔒",
    layout="wide"
)

# --- Password Protection ---
# Load password from Streamlit secrets or environment variable
APP_PASSWORD = os.environ.get("ST_PASSWORD") or st.secrets.get("app_password")

if not APP_PASSWORD:
    st.error("App password not set. Please set ST_PASSWORD environment variable or app_password in st.secrets.")
    st.stop()

entered_password = st.text_input("Enter password", type="password")

if entered_password != APP_PASSWORD:
    st.error("Incorrect password. Please try again.")
    st.stop() # Stop execution if password is not correct

# --- Session State Initialization ---
if 'sql_query' not in st.session_state:
    st.session_state.sql_query = ""
if 'sanitized_output' not in st.session_state:
    st.session_state.sanitized_output = ""
if 'transformation_log' not in st.session_state:
    st.session_state.transformation_log = {}
if 'uploaded_db' not in st.session_state:
    st.session_state.uploaded_db = None
if 'db_mapping' not in st.session_state:
    st.session_state.db_mapping = None
if 'selected_dialect' not in st.session_state:
    st.session_state.selected_dialect = 'postgres'  # Changed from 'standard' to 'postgres'

# --- Sample Data ---
SAMPLE_QUERY = """
SELECT c.customer_name, o.order_total
FROM finance_db.customers c
JOIN finance_db.orders o ON c.customer_id = o.customer_id
WHERE c.email = 'john.doe@example.com' AND o.order_total > 500;
"""

# --- Helper Functions ---
def save_uploaded_db(uploaded_file):
    """Save uploaded database file to a temporary location."""
    temp_path = f"temp_{uploaded_file.name}"
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return temp_path

def cleanup_temp_files(temp_path):
    """Remove temporary files."""
    if os.path.exists(temp_path):
        os.remove(temp_path)

# --- Main App ---
st.title("SQL Anonymizer")
st.subheader("Safely anonymize SQL queries and database schemas")

# Create tabs for different functionalities
tab1, tab2 = st.tabs(["Query Anonymizer", "Database Schema Anonymizer"])

# --- Query Anonymizer Tab ---
with tab1:
    st.header("SQL Query Anonymizer")
    
    # Input Area
    sql_input = st.text_area(
        "Paste your SQL query here:",
        value=st.session_state.sql_query,
        height=250,
        help="Enter the SQL query you want to anonymize.",
        key="sql_input_area"
    )

    # Options
    col_options_1, col_options_2 = st.columns(2) # Use columns for layout

    with col_options_1:
        show_explanation = st.checkbox("Show explanation/mapping", value=True, key="show_explanation_checkbox")

    with col_options_2:
        # Add Dialect Selection
        dialect_options = ['postgres', 'mysql', 'sqlite', 'bigquery', 'snowflake', 'redshift', 'oracle', 'tsql', 'spark', 'hive', 'presto', 'drill', 'teradata']
        selected_dialect = st.selectbox(
            "Select SQL Dialect:",
            dialect_options,
            index=dialect_options.index(st.session_state.selected_dialect),
            key="dialect_select"
        )
        st.session_state.selected_dialect = selected_dialect # Update session state

    # Buttons
    col1, col2 = st.columns([0.3, 0.7])

    with col1:
        if st.button("Anonymize SQL", key="anonymize_button"):
            if sql_input:
                try:
                    # Pass the selected dialect to the anonymizer
                    anonymizer = SQLAnonymizer(dialect=st.session_state.selected_dialect)
                    sanitized_query, transformation_log = anonymizer.sanitize_sql(sql_input, explain=True)
                    st.session_state.sanitized_output = sanitized_query
                    st.session_state.transformation_log = transformation_log
                except Exception as e:
                    st.error(f"Error during anonymization: {str(e)}")
                    st.session_state.transformation_log = {"error": str(e)} # Store error in log
            else:
                st.warning("Please paste an SQL query to anonymize.")

    with col2:
        if st.button("Try Sample Query", key="sample_button"):
            st.session_state.sql_query = SAMPLE_QUERY
            st.session_state.sanitized_output = ""
            st.session_state.transformation_log = {}
            st.rerun()

    # Display Results
    if st.session_state.sanitized_output:
        st.subheader("Sanitized SQL Query:")
        st.code(st.session_state.sanitized_output, language="sql")

        # Download button for sanitized query
        st.download_button(
            label="Download Sanitized SQL",
            data=st.session_state.sanitized_output.encode('utf-8'),
            file_name="sanitized_query.sql",
            mime="text/sql",
            help="Download the anonymized query as a .sql file."
        )

        if show_explanation and st.session_state.transformation_log:
            st.subheader("Transformation Mapping:")
            if "error" in st.session_state.transformation_log:
                st.error(f"Could not generate mapping due to an error: {st.session_state.transformation_log['error']}")
            else:
                # Display the refined log structure
                mapping_log = st.session_state.transformation_log
                with st.expander("View Mapping Details", expanded=True): # Expand by default
                    if mapping_log.get("identifier_mappings"):
                        st.json({"Identifier Mappings": mapping_log["identifier_mappings"]})
                    if mapping_log.get("column_mappings_per_table"):
                         # Reformat for display: Anonymized Table Name -> {Original Col: Anonymized Col}
                         formatted_col_maps_display: Dict[str, Dict[str, str]] = {}
                         table_id_map = mapping_log.get("identifier_mappings", {}).get("table", {})
                         for original_table_path, col_map in mapping_log["column_mappings_per_table"].items():
                             anonymized_table_name = table_id_map.get(original_table_path, original_table_path) # Get anonymized table name
                             formatted_col_maps_display[anonymized_table_name] = col_map
                         st.json({"Column Mappings (per anonymized table)": formatted_col_maps_display})
                    if mapping_log.get("literal_mappings"):
                        st.json({"Literal Mappings": mapping_log["literal_mappings"]})
                    # Display global column fallback if present
                    if mapping_log.get("identifier_mappings", {}).get("global_column_fallback"):
                         st.json({"Global Column Fallback Mapping (for unqualified columns)": mapping_log["identifier_mappings"]["global_column_fallback"]})

# --- Database Schema Anonymizer Tab ---
with tab2:
    st.header("SQLite Database Schema Anonymizer")
    
    uploaded_file = st.file_uploader("Upload SQLite Database", type=['db', 'sqlite', 'sqlite3'])
    
    if uploaded_file:
        temp_input_path = save_uploaded_db(uploaded_file)
        output_path = f"anonymized_{uploaded_file.name}"
        
        if st.button("Anonymize Database Schema"):
            try:
                with st.spinner("Anonymizing database schema..."):
                    # Run the anonymization process
                    mapping = anonymize_sqlite_schema(temp_input_path, output_path)
                    
                    if "error" not in mapping:
                        st.session_state.db_mapping = mapping
                        
                        # Display results
                        st.success("Database schema anonymization complete!")
                        
                        # Show mapping
                        st.subheader("Transformation Mapping:")
                        with st.expander("View Table and Column Mappings"):
                            st.json(mapping)
                        
                        # Download buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            with open(output_path, 'rb') as f:
                                st.download_button(
                                    label="Download Anonymized Database",
                                    data=f,
                                    file_name=output_path,
                                    mime="application/x-sqlite3"
                                )
                        with col2:
                            st.download_button(
                                label="Download Mapping JSON",
                                data=json.dumps(mapping, indent=4).encode('utf-8'),
                                file_name="mapping.json",
                                mime="application/json"
                            )
                    else:
                        st.error(f"Error during anonymization: {mapping['error']}")
                
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
            finally:
                # Cleanup temporary files
                cleanup_temp_files(temp_input_path)
                if os.path.exists(output_path):
                    cleanup_temp_files(output_path)

# --- Sidebar ---
st.sidebar.header("About")
st.sidebar.info("""
This tool helps anonymize SQL queries and database schemas by replacing sensitive information with generic placeholders.

Features:
- SQL Query Anonymization
- SQLite Database Schema Anonymization
- Mapping Export
- Sample Queries
""")
st.sidebar.markdown("Powered by `sqlglot` + `Streamlit` + `sqlite3`")
st.sidebar.markdown("Created for educational and demonstration purposes.")

# Update session state for query input
if sql_input != st.session_state.sql_query and st.session_state.sql_query != SAMPLE_QUERY:
    st.session_state.sql_query = sql_input