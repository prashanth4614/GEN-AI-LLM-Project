from fpdf import FPDF
import tempfile
import streamlit as st
import pandas as pd
import mysql.connector
from docx import Document
from google import genai
import os

# --- Session State Initialization ---
if 'action_log' not in st.session_state:
    st.session_state['action_log'] = []

# --- Configuration ---

API_KEY = os.getenv('GENAI_API_KEY')
MYSQL_HOST = 'mysql-2197727f-prashanthm-60ad.j.aivencloud.com'
MYSQL_User = 'avnadmin'
MYSQL_PWD = os.getenv('MYSQL_PWD')

MYSQL_PORT = 20367
DEFAULT_DB = "genai_migration"

def ensure_database_exists():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_User,
            password=MYSQL_PWD,
            port=MYSQL_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DEFAULT_DB}")
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"MySQL connection or database creation failed: {e}")
        raise

def get_db_connection():
    try:
        ensure_database_exists()
        return mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_User,
            password=MYSQL_PWD,
            port=MYSQL_PORT,
            database=DEFAULT_DB
        )
    except Exception as e:
        st.error(f"MySQL connection failed: {e}")
        raise

system_instructions = (
    "Output only the SQL query, with no additional explanation or text. The output should be ready to execute directly in MySQL."
)

def get_docx_text(file_path):
    doc = Document(file_path)
    return "\n".join([para.text for para in doc.paragraphs])


def get_db_table_schema():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        schema_info = []
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            schema_info.append(f"Table: {table}")
            for col in columns:
                schema_info.append(f"  {col[0]} {col[1]} {'NOT NULL' if col[2]=='NO' else ''} {'PRIMARY KEY' if col[3]=='PRI' else ''}")
        cursor.close()
        conn.close()
        return '\n'.join(schema_info)
    except Exception as e:
        return "[Could not fetch schema from DB: " + str(e) + "]"

def llm_connect(user_prompt, docx_path=None, table_context=None):
    doc_text = get_docx_text(docx_path) if docx_path else ""
    # Add table context for all menus except Schema Generation
    table_info = ""
    if table_context:
        table_info = (
            "\n\nOnly generate SQL for the following tables and columns (from the current database):\n"
            f"{table_context}\n"
            "If the prompt is not related to these tables, respond: 'Prompt not related to available tables.'\n"
        )
    Prompt = system_instructions + "\n" + doc_text + table_info + "\n" + user_prompt
    client = genai.Client(api_key=API_KEY)
    try:
        result = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=[Prompt],
        )
        text = result.text.strip() if hasattr(result, 'text') else str(result)
        # Remove code block notations if present
        text = text.replace("```sql", "").replace("```", "").strip()
        return text
    except Exception as e:
        import traceback
        st.error("[LLM API Error] Service unavailable or request failed. Please try again later.")
        st.exception(e)
        return None

def execute_sql(sql, fetch=False, show_details=False):
    details = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            for statement in sql.split(';'):
                stmt = statement.strip()
                if stmt:
                    try:
                        cursor.execute(stmt)
                        details.append(f"✅ Executed: {stmt[:100]}{'...' if len(stmt)>100 else ''}")
                    except Exception as stmt_err:
                        details.append(f"❌ Error in: {stmt[:100]}{'...' if len(stmt)>100 else ''}\nError: {stmt_err}")
            if fetch:
                result = cursor.fetchall()
                return result, details
        finally:
            cursor.close()
            conn.close()
        if not fetch:
            return None, details
    except Exception as e:
        details.append(f"MySQL execution error: {e}")
        st.error(f"MySQL execution error: {e}")
        raise
    return None, details


# --- Streamlit App ---
st.set_page_config(page_title="GenAI Data Migration & BI", layout="wide")
st.title("GenAI-Assisted Data Migration & BI Platform")

# MySQL connection check
mysql_connected = False
if st.sidebar.button("Check MySQL Connection"):
    try:
        conn = get_db_connection()
        conn.close()
        st.sidebar.success("MySQL connection successful!")
        mysql_connected = True
    except Exception as e:
        st.sidebar.error(f"MySQL connection failed: {e}")
        mysql_connected = False

if not mysql_connected:
    # Try to check connection automatically on app load
    try:
        conn = get_db_connection()
        conn.close()
        mysql_connected = True
        st.sidebar.success("MySQL connection successful!")
    except Exception as e:
        st.sidebar.error(f"MySQL connection failed: {e}")
        mysql_connected = False

menu = st.sidebar.radio("Select Task", [
    "Schema Generation",
    "Data Import & Validation",
    "Query & Logic Translation",
    "Automated Testing",
    "BI Reporting & Visualization",
    "Documentation & Reflection"
])

if menu == "Schema Generation":
    st.header("1. Schema Design with GenAI")
    st.info("The LLM will always use dataset_info.docx as context. Optionally, select one or more CSV files to provide sample data for schema inference.")
    user_prompt = st.text_area("Prompt for Schema Generation", "Generate Drop and DDL statements for all tables.")
    csv_files = ["CUSTOMERS.csv", "INVENTORY.csv", "SALES.csv"]
    selected_csvs = st.multiselect("Select CSV files to provide sample data context", csv_files)
    # Always use dataset_info.docx as context
    docx_path = "dataset_info.docx"
    # Prepare sample data context from selected CSVs
    csv_context = ""
    for csv_file in selected_csvs:
        try:
            df = pd.read_csv(csv_file, nrows=5)
            csv_context += f"\nSample data from {csv_file}:\n"
            csv_context += df.head().to_csv(index=False)
        except Exception as e:
            csv_context += f"\n[Could not read {csv_file}: {e}]\n"
    if 'schema_sql' not in st.session_state:
        st.session_state['schema_sql'] = None
    if st.button("Generate Schema"):
        # Append CSV sample data to user prompt
        full_prompt = user_prompt + "\n" + csv_context if csv_context else user_prompt
        sql = llm_connect(full_prompt, docx_path)
        st.session_state['schema_sql'] = sql
        st.code(sql, language="sql")
        st.session_state['action_log'].append({
            'section': 'Schema Generation',
            'input': full_prompt,
            'output': sql
        })
    # Show code if already generated
    if st.session_state['schema_sql']:
        st.code(st.session_state['schema_sql'], language="sql")
        if not mysql_connected:
            st.warning("MySQL connection not available. Please check connection before running SQL.")
        else:
            if st.button("Run in MySQL (Execute Schema)"):
                try:
                    _, details = execute_sql(st.session_state['schema_sql'], show_details=True)
                    st.markdown("**Execution Details:**")
                    for line in details:
                        if line.startswith("✅"):
                            st.success(line)
                        elif line.startswith("❌"):
                            st.error(line)
                        else:
                            st.info(line)
                    st.session_state['action_log'].append({
                        'section': 'Schema Generation (Execution)',
                        'input': st.session_state['schema_sql'],
                        'output': '\n'.join(details)
                    })
                    if all(l.startswith("✅") for l in details if l.strip()):
                        st.success("All statements executed successfully.")
                    elif any(l.startswith("❌") for l in details):
                        st.warning("Some statements failed. See details above.")
                except Exception as e:
                    st.error(f"Error executing schema: {e}")

elif menu == "Data Import & Validation":
    st.header("2. Data Import & Validation")
    st.info("Select a CSV file from the workspace and import it into the corresponding table. Then validate row counts.")
    file_table_map = {
        "CUSTOMERS.csv": "CUSTOMERS",
        "INVENTORY.csv": "INVENTORY",
        "SALES.csv": "SALES"
    }
    csv_files = list(file_table_map.keys())
    selected_csv = st.selectbox("Select CSV file to import", csv_files)
    target_table = file_table_map[selected_csv]
    st.write(f"Target table: `{target_table}`")
    import_status = None
    if not mysql_connected:
        st.warning("MySQL connection not available. Please check connection before running SQL.")
    else:
        if st.button("Import Selected CSV to MySQL"):
            try:
                df = pd.read_csv(selected_csv)
                conn = get_db_connection()
                cursor = conn.cursor()
                import_details = []
                for idx, row in df.iterrows():
                    placeholders = ','.join(['%s'] * len(row))
                    sql_insert = f"INSERT INTO {target_table} VALUES ({placeholders})"
                    try:
                        cursor.execute(sql_insert, tuple(row))
                        import_details.append(f"✅ Row {idx+1} inserted.")
                    except Exception as row_err:
                        import_details.append(f"❌ Row {idx+1} failed: {row_err}")
                conn.commit()
                cursor.close()
                conn.close()
                st.markdown("**Import Details:**")
                for line in import_details[:20]:
                    if line.startswith("✅"):
                        st.success(line)
                    else:
                        st.error(line)
                if len(import_details) > 20:
                    st.info(f"...and {len(import_details)-20} more rows.")
                import_status = f"Imported {sum(1 for l in import_details if l.startswith('✅'))} rows into {target_table}."
                st.success(import_status)
            except Exception as e:
                st.error(f"Import failed: {e}")
        if st.button("Validate Row Count"):
            try:
                df_rows = len(pd.read_csv(selected_csv))
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                db_rows = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                st.info(f"CSV rows: {df_rows}, DB rows: {db_rows}")
            except Exception as e:
                st.error(f"Validation failed: {e}")

elif menu == "Query & Logic Translation":
    st.header("3. Query & Logic Translation")
    st.info("Select the Oracle PL/SQL file from the workspace to translate.")
    sql_file = st.selectbox("Select Oracle PL/SQL file", ["oracle_plsql_procedures.sql"])
    plsql_code = None
    if sql_file:
        with open(sql_file, "r") as f:
            plsql_code = f.read()
    if 'query_sql' not in st.session_state:
        st.session_state['query_sql'] = None
    # Get table context from DB schema
    table_context = get_db_table_schema()
    if st.button("Translate to MySQL SQL") and plsql_code:
        sql = llm_connect(f"Translate this logic to MySQL: {plsql_code}", table_context=table_context)
        st.session_state['query_sql'] = sql
        st.code(sql, language="sql")
        st.session_state['action_log'].append({
            'section': 'Query & Logic Translation',
            'input': plsql_code,
            'output': sql
        })
    if st.session_state['query_sql']:
        st.code(st.session_state['query_sql'], language="sql")
        if not mysql_connected:
            st.warning("MySQL connection not available. Please check connection before running SQL.")
        else:
            if st.button("Run in MySQL (Execute Query)"):
                try:
                    result, details = execute_sql(st.session_state['query_sql'], fetch=True, show_details=True)
                    st.markdown("**Execution Details:**")
                    for line in details:
                        if line.startswith("✅"):
                            st.success(line)
                        elif line.startswith("❌"):
                            st.error(line)
                        else:
                            st.info(line)
                    st.session_state['action_log'].append({
                        'section': 'Query & Logic Translation (Execution)',
                        'input': st.session_state['query_sql'],
                        'output': '\n'.join(details)
                    })
                    if result:
                        st.dataframe(result)
                    st.success("Query executed. See details above.")
                except Exception as e:
                    st.error(f"Error executing query: {e}")

elif menu == "Automated Testing":
    st.header("4. Automated Testing with GenAI")
    test_prompt = st.text_area("Describe a data test or check:", "Find orders without invoices.")
    if 'test_sql' not in st.session_state:
        st.session_state['test_sql'] = None
    table_context = get_db_table_schema()
    if st.button("Generate Test Query") and test_prompt:
        sql = llm_connect(f"Write a test SQL for: {test_prompt}", table_context=table_context)
        st.session_state['test_sql'] = sql
        st.code(sql, language="sql")
        st.session_state['action_log'].append({
            'section': 'Automated Testing',
            'input': test_prompt,
            'output': sql
        })
    if st.session_state['test_sql']:
        st.code(st.session_state['test_sql'], language="sql")
        if not mysql_connected:
            st.warning("MySQL connection not available. Please check connection before running SQL.")
        else:
            if st.button("Run in MySQL (Execute Test)"):
                try:
                    result, details = execute_sql(st.session_state['test_sql'], fetch=True, show_details=True)
                    st.markdown("**Execution Details:**")
                    for line in details:
                        if line.startswith("✅"):
                            st.success(line)
                        elif line.startswith("❌"):
                            st.error(line)
                        else:
                            st.info(line)
                    st.session_state['action_log'].append({
                        'section': 'Automated Testing (Execution)',
                        'input': st.session_state['test_sql'],
                        'output': '\n'.join(details)
                    })
                    if result:
                        st.dataframe(result)
                    st.success("Test query executed. See details above.")
                except Exception as e:
                    st.error(f"Error executing test query: {e}")

elif menu == "BI Reporting & Visualization":
    st.header("5. BI Reporting & Visualization")
    analysis_prompt = st.text_area("Describe the analysis or KPI you want:", "Show monthly sales totals.")
    if 'bi_sql' not in st.session_state:
        st.session_state['bi_sql'] = None
    table_context = get_db_table_schema()
    if st.button("Generate & Run Analysis") and analysis_prompt:
        sql = llm_connect(f"Write a MySQL query for: {analysis_prompt}", table_context=table_context)
        st.session_state['bi_sql'] = sql
        st.code(sql, language="sql")
        st.session_state['action_log'].append({
            'section': 'BI Reporting & Visualization',
            'input': analysis_prompt,
            'output': sql
        })
    if st.session_state['bi_sql']:
        st.code(st.session_state['bi_sql'], language="sql")
        if not mysql_connected:
            st.warning("MySQL connection not available. Please check connection before running SQL.")
        else:
            if st.button("Run in MySQL (Show Results)"):
                try:
                    result, details = execute_sql(st.session_state['bi_sql'], fetch=True, show_details=True)
                    st.markdown("**Execution Details:**")
                    for line in details:
                        if line.startswith("✅"):
                            st.success(line)
                        elif line.startswith("❌"):
                            st.error(line)
                        else:
                            st.info(line)
                    st.session_state['action_log'].append({
                        'section': 'BI Reporting & Visualization (Execution)',
                        'input': st.session_state['bi_sql'],
                        'output': '\n'.join(details)
                    })
                    if result:
                        df = pd.DataFrame(result)
                        st.dataframe(df)
                        # Show both bar and line charts for numeric columns
                        numeric_df = df.select_dtypes(include='number')
                        if not df.empty and not numeric_df.empty:
                            st.subheader("Bar Chart")
                            st.bar_chart(numeric_df)
                            st.subheader("Line Chart")
                            st.line_chart(numeric_df)
                    st.success("Analysis query executed and visualized. See details above.")
                except Exception as e:
                    st.error(f"Error executing analysis query: {e}")
    st.info("You can use Streamlit's chart features to visualize query results.")

elif menu == "Documentation & Reflection":
    st.header("6. Documentation & Reflection")
    st.markdown("""
    - **Migration Steps:** Document each phase of your migration.
    - **How GenAI Helped:** Summarize GenAI's role in schema, logic, and testing.
    - **Lessons Learned:** Reflect on challenges and solutions.
    """)
    st.info("Add your project documentation and reflections here.")

    # PDF generation and download
    if st.button("Generate & Download PDF Report"):
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", 'B', 16)
        pdf.cell(0, 10, "GenAI-Assisted Data Migration & BI Report", ln=True, align='C')
        pdf.ln(10)
        pdf.set_font("Arial", '', 12)
        for entry in st.session_state['action_log']:
            pdf.set_font("Arial", 'B', 13)
            pdf.cell(0, 10, entry['section'], ln=True)
            pdf.set_font("Arial", 'I', 11)
            # Replace Unicode check/cross with plain text for PDF
            input_text = entry['input'].replace('✅', 'SUCCESS:').replace('❌', 'ERROR:') if entry['input'] else ''
            output_text = entry['output'].replace('✅', 'SUCCESS:').replace('❌', 'ERROR:') if entry['output'] else ''
            pdf.multi_cell(0, 8, f"Input:\n{input_text}")
            pdf.set_font("Arial", '', 11)
            pdf.multi_cell(0, 8, f"Output/Result:\n{output_text}")
            pdf.ln(4)
        # Save to temp file and offer download
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmpfile:
            pdf.output(tmpfile.name)
            tmpfile.flush()
            with open(tmpfile.name, "rb") as f:
                st.download_button(
                    label="Download PDF Report",
                    data=f.read(),
                    file_name="genai_migration_report.pdf",
                    mime="application/pdf"
                )
