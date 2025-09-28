from fpdf import FPDF
import tempfile
import streamlit as st
import pandas as pd
import mysql.connector
from docx import Document
from google import genai
import os
from decimal import Decimal
import decimal

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
            "\nIMPORTANT: For stored procedures:\n"
            "1. Do NOT include DELIMITER statements\n"
            "2. Use standard MySQL procedure syntax\n"
            "3. Use IN/OUT parameters properly\n"
            "Example format:\n"
            "CREATE PROCEDURE procedure_name(IN param1 TYPE, OUT param2 TYPE)\n"
            "BEGIN\n"
            "    -- procedure body\n"
            "END;\n"
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
            # Split on semicolon but preserve content between BEGIN/END blocks
            statements = []
            current_stmt = []
            in_procedure = False
            
            for line in sql.split('\n'):
                line = line.strip()
                if not line:
                    continue
                    
                if 'CREATE PROCEDURE' in line or 'CREATE FUNCTION' in line:
                    in_procedure = True
                    current_stmt = [line]
                elif in_procedure:
                    current_stmt.append(line)
                    if line.startswith('END;'):
                        statements.append('\n'.join(current_stmt))
                        in_procedure = False
                        current_stmt = []
                else:
                    if line.endswith(';'):
                        current_stmt.append(line)
                        statements.append('\n'.join(current_stmt))
                        current_stmt = []
                    else:
                        current_stmt.append(line)
            
            if current_stmt:  # Add any remaining statement
                statements.append('\n'.join(current_stmt))
            
            for stmt in statements:
                stmt = stmt.strip()
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
    analysis_prompt = st.text_area("Describe the analysis or KPI you want:", 
        "Show monthly sales totals for the past year. Include month, total quantity sold, and total amount.")
    if 'bi_sql' not in st.session_state:
        st.session_state['bi_sql'] = None
    table_context = get_db_table_schema()
    if st.button("Generate & Run Analysis") and analysis_prompt:
        enhanced_prompt = f"""
Write a MySQL query for: {analysis_prompt}
IMPORTANT: 
1. Use DATE_FORMAT for date/time columns and include them in GROUP BY clause. Example:
   SELECT DATE_FORMAT(date_col, '%Y-%m') as month, SUM(amount)
   GROUP BY DATE_FORMAT(date_col, '%Y-%m')
2. Include multiple numeric aggregates (SUM, AVG, COUNT etc.)
3. Always order by the formatted date/time column
4. Any non-aggregated column in SELECT must be in GROUP BY
5. Ensure proper GROUP BY clause for all non-aggregated columns
"""
        sql = llm_connect(enhanced_prompt, table_context=table_context)
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
                   
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(st.session_state['bi_sql'])
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    details = [f"✅ Executed: {st.session_state['bi_sql'][:100]}{'...' if len(st.session_state['bi_sql'])>100 else ''}"]
                    cursor.close()
                    conn.close()
                    st.markdown("**Execution Details:**")
                    for line in details:
                        st.success(line)
                    st.session_state['action_log'].append({
                        'section': 'BI Reporting & Visualization (Execution)',
                        'input': st.session_state['bi_sql'],
                        'output': '\n'.join(details)
                    })
                    if result and columns:
                        # Convert all values in result to handle Decimal type
                        processed_result = []
                        for row in result:
                            processed_row = []
                            for value in row:
                                # Convert Decimal to float
                                if isinstance(value, (Decimal, decimal.Decimal)):
                                    processed_row.append(float(value))
                                else:
                                    processed_row.append(value)
                            processed_result.append(processed_row)
                        
                        # Create DataFrame with processed values
                        df = pd.DataFrame(processed_result, columns=columns)
                        
                        # Debug information
                        st.write("**Data Overview:**")
                        st.write(f"- Shape: {df.shape}")
                        st.write(f"- Columns: {list(df.columns)}")
                        st.write(f"- Data Types: {df.dtypes.to_dict()}")
                        
                        # Display the data table
                        st.dataframe(df)
                        
                        # Identify numeric columns (excluding any index/id columns)
                        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
                        numeric_cols = [col for col in numeric_cols if not col.lower().endswith('_id')]
                        
                        if not df.empty and len(numeric_cols) > 0:
                            st.write(f"**Plotting numeric columns:** {list(numeric_cols)}")
                            
                            # For time series, use the first column as index
                            index_col = df.columns[0]  # Usually the date/time column
                            
                            # Create visualization dataframe
                            plot_df = df.set_index(index_col)[numeric_cols]
                            
                            # Create temporary directory for chart images if it doesn't exist
                            import os
                            temp_dir = "temp_charts"
                            if not os.path.exists(temp_dir):
                                os.makedirs(temp_dir)
                            
                            # Save charts as images using matplotlib
                            import matplotlib.pyplot as plt
                            
                            # Bar Chart
                            st.subheader("Bar Chart")
                            fig1, ax1 = plt.subplots(figsize=(10, 6))
                            plot_df.plot(kind='bar', ax=ax1)
                            plt.xticks(rotation=45)
                            plt.tight_layout()
                            bar_chart_path = os.path.join(temp_dir, "bar_chart.png")
                            plt.savefig(bar_chart_path)
                            st.bar_chart(plot_df)
                            
                            # Line Chart
                            st.subheader("Line Chart")
                            fig2, ax2 = plt.subplots(figsize=(10, 6))
                            plot_df.plot(kind='line', ax=ax2)
                            plt.xticks(rotation=45)
                            plt.tight_layout()
                            line_chart_path = os.path.join(temp_dir, "line_chart.png")
                            plt.savefig(line_chart_path)
                            st.line_chart(plot_df)
                            
                            # Store chart paths in session state for PDF generation
                            if 'chart_paths' not in st.session_state:
                                st.session_state['chart_paths'] = []
                            st.session_state['chart_paths'] = [bar_chart_path, line_chart_path]
                            
                            # Store the DataFrame in session state for PDF generation
                            st.session_state['last_query_df'] = df
                        else:
                            st.info("No numeric columns available for charting. Please modify your query to include numeric aggregates (e.g., SUM, AVG, COUNT).")
                    else:
                        st.info("No results returned from query.")
                    st.success("Analysis query executed and visualized. See details above.")
                except Exception as e:
                    st.error(f"Error executing analysis query: {e}")
    st.info("You can use Streamlit's chart features to visualize query results.")

elif menu == "Documentation & Reflection":
    st.header("6. Documentation & Reflection")
    
    st.subheader("Project Overview")
    st.markdown("""
    This GenAI-assisted data migration and BI platform demonstrates an innovative approach to database migration
    and analytics, leveraging artificial intelligence for various aspects of the process. The project showcases
    the integration of modern technologies including:
    
    - **Generative AI (Gemini Pro)** for intelligent SQL generation and translation
    - **MySQL** as the target database system
    - **Streamlit** for the interactive web interface
    - **Pandas** for data handling and transformation
    - **FPDF** for documentation generation
    """)
    
    st.subheader("Migration Steps & Workflow")
    st.markdown("""
    1. **Schema Design**
       - Automated DDL generation from requirements document
       - Sample data analysis for schema optimization
       - Schema validation and deployment
    
    2. **Data Import & Validation**
       - CSV file import with error handling
       - Row count validation
       - Data integrity checks
    
    3. **Query & Logic Translation**
       - Oracle PL/SQL to MySQL translation
       - Automated query optimization
       - Execution validation
    
    4. **Automated Testing**
       - Data quality checks
       - Business rule validation
       - Performance testing
    
    5. **BI Reporting & Visualization**
       - KPI monitoring
       - Interactive charts and graphs
       - Real-time analytics
    """)
    
    st.subheader("How GenAI Helped")
    st.markdown("""
    Generative AI played a crucial role in:
    
    1. **Schema Generation**
       - Understanding business requirements from text
       - Generating optimal table structures
       - Creating correct data types and relationships
    
    2. **Code Translation**
       - Converting Oracle PL/SQL to MySQL syntax
       - Maintaining business logic integrity
       - Optimizing queries for performance
    
    3. **Query Generation**
       - Creating complex SQL from natural language
       - Understanding context and relationships
       - Generating optimized queries
    
    4. **Testing & Validation**
       - Generating comprehensive test cases
       - Identifying edge cases
       - Validating data integrity
    """)
    
    st.subheader("Technical Implementation")
    st.markdown("""
    The platform implements several key technical features:
    
    1. **Error Handling**
       - Comprehensive try-catch blocks
       - Detailed error reporting
       - User-friendly error messages
    
    2. **State Management**
       - Session state for query persistence
       - Action logging
       - Progress tracking
    
    3. **Data Visualization**
       - Interactive charts
       - Real-time data updates
       - Multiple visualization options
    
    4. **Documentation**
       - Automated PDF report generation
       - Action logging and tracking
       - Execution history
    """)
    
    st.subheader("Lessons Learned")
    st.markdown("""
    Key insights from the project:
    
    1. **GenAI Integration**
       - Context-aware prompts improve accuracy
       - Error handling is crucial for AI responses
       - Validation of AI-generated code is essential
    
    2. **Data Migration**
       - Incremental validation improves reliability
       - Type handling needs special attention
       - Performance optimization is critical
    
    3. **User Experience**
       - Clear feedback improves usability
       - Progress indication is important
       - Error messages should be actionable
    
    4. **Best Practices**
       - Regular testing throughout migration
       - Comprehensive logging for auditing
       - Clear documentation of processes
    """)
    
    st.subheader("Future Enhancements")
    st.markdown("""
    Potential areas for improvement:
    
    1. **Additional Features**
       - Support for more database systems
       - Advanced data transformation options
       - Enhanced visualization capabilities
    
    2. **Performance Optimization**
       - Batch processing for large datasets
       - Query optimization techniques
       - Caching strategies
    
    3. **User Interface**
       - More interactive visualizations
       - Customizable dashboards
       - Enhanced error reporting
    """)
    
    st.info("Download the complete project report using the button below for detailed documentation including all executed steps and their results.")

    # PDF generation and download
    if st.button("Generate & Download PDF Report"):
        # Initialize PDF with UTF-8 encoding
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Set UTF-8 encoding
        pdf.add_font('DejaVu', '', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', uni=True)
        pdf.set_font('DejaVu', '', 14)
        
        # Title Page
        pdf.add_page()
        pdf.set_font('DejaVu', '', 20)
        pdf.cell(0, 20, "GenAI-Assisted Data Migration & BI", ln=True, align='C')
        pdf.ln(10)
        pdf.set_font('DejaVu', '', 16)
        pdf.cell(0, 10, "Project Execution Report", ln=True, align='C')
        pdf.ln(10)
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0, 10, f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%-d %H:%M:%S')}", ln=True, align='C')
        
        # Table of Contents
        pdf.add_page()
        pdf.set_font('DejaVu', '', 16)
        pdf.cell(0, 10, "Table of Contents", ln=True)
        pdf.ln(5)
        
        # Group entries by section for TOC
        sections = {}
        for entry in st.session_state['action_log']:
            section = entry['section'].split(' (')[0]  # Remove any parentheses part
            if section not in sections:
                sections[section] = []
            sections[section].append(entry)
        
        # Write TOC
        pdf.set_font("Arial", '', 12)
        page_num = 3  # Start from page 3 (after title and TOC)
        toc_entries = []
        for section in sections.keys():
            toc_entries.append((section, page_num))
            page_num += len(sections[section]) + 1  # +1 for section title page
        
        for title, page in toc_entries:
            pdf.cell(0, 10, f"{title} {'.' * (50 - len(title))} {page}", ln=True)
        
        # Content Pages - Section by Section
        for section_name, entries in sections.items():
            pdf.add_page()
            # Section Title
            pdf.set_font("Arial", 'B', 16)
            pdf.cell(0, 15, section_name, ln=True)
            pdf.ln(5)
            
            # Section Content
            for entry in entries:
                # Skip execution entries if we have the main entry for any section
                if "(Execution)" in entry['section']:
                    # Check if we have a corresponding main entry
                    base_section = entry['section'].split(" (Execution)")[0]
                    if any(e['section'] == base_section for e in entries):
                        continue
                    
                # Entry title with timestamp if available
                pdf.set_font("Arial", 'B', 14)
                pdf.cell(0, 10, f"Execution Step", ln=True)
                pdf.ln(5)
                
                # Input
                pdf.set_font('DejaVu', '', 12)
                pdf.cell(0, 10, "Input:", ln=True)
                pdf.set_font('DejaVu', '', 11)
                input_text = entry['input'].replace('✅', '[SUCCESS]').replace('❌', '[ERROR]') if entry['input'] else 'N/A'
                pdf.multi_cell(0, 8, input_text)
                pdf.ln(5)
                
                # Output
                pdf.set_font('DejaVu', '', 12)
                pdf.cell(0, 10, "Output/Result:", ln=True)
                pdf.set_font('DejaVu', '', 11)
                output_text = entry['output'].replace('✅', '[SUCCESS]').replace('❌', '[ERROR]') if entry['output'] else 'N/A'
                pdf.multi_cell(0, 8, output_text)
                
                # Add query results if available (only for BI visualization section)
                if section_name == "BI Reporting & Visualization" and not "(Execution)" in entry['section'] and 'last_query_df' in st.session_state:
                    pdf.add_page()
                    pdf.set_font('DejaVu', '', 14)
                    pdf.cell(0, 10, "Query Results", ln=True)
                    pdf.ln(5)
                    
                    # Add DataFrame as table
                    df = st.session_state['last_query_df']
                    # Convert DataFrame to string with proper formatting
                    pdf.set_font('DejaVu', '', 10)
                    
                    # Calculate column widths
                    col_widths = [pdf.get_string_width(str(col)) + 6 for col in df.columns]
                    for _, row in df.iterrows():
                        for i, val in enumerate(row):
                            col_widths[i] = max(col_widths[i], pdf.get_string_width(str(val)) + 6)
                    
                    # Ensure total width doesn't exceed page width
                    available_width = pdf.w - 2*pdf.l_margin
                    if sum(col_widths) > available_width:
                        scale = available_width / sum(col_widths)
                        col_widths = [w * scale for w in col_widths]
                    
                    # Add headers
                    for i, col in enumerate(df.columns):
                        pdf.cell(col_widths[i], 7, str(col), 1, 0, 'C')
                    pdf.ln()
                    
                    # Add data rows
                    for _, row in df.iterrows():
                        for i, val in enumerate(row):
                            pdf.cell(col_widths[i], 6, str(val), 1)
                        pdf.ln()
                    
                    # Add visualizations if available
                    if 'chart_paths' in st.session_state and st.session_state['chart_paths']:
                        pdf.add_page()
                        pdf.set_font('DejaVu', '', 14)
                        pdf.cell(0, 10, "Visualizations", ln=True)
                        pdf.ln(5)
                        
                        for i, chart_path in enumerate(st.session_state['chart_paths']):
                            if os.path.exists(chart_path):
                                chart_type = "Bar Chart" if "bar" in chart_path else "Line Chart"
                                pdf.set_font('DejaVu', '', 12)
                                pdf.cell(0, 10, chart_type, ln=True)
                                # Add image with proper scaling
                                pdf.image(chart_path, x=10, w=190)
                                pdf.ln(10)
                
                # Status indicators
                if '✅' in str(entry['output']) or 'success' in str(entry['output']).lower():
                    pdf.set_font('DejaVu', '', 11)
                    pdf.set_text_color(0, 128, 0)  # Green
                    pdf.cell(0, 10, "Status: Success", ln=True)
                elif '❌' in str(entry['output']) or 'error' in str(entry['output']).lower():
                    pdf.set_font('DejaVu', '', 11)
                    pdf.set_text_color(255, 0, 0)  # Red
                    pdf.cell(0, 10, "Status: Error", ln=True)
                
                pdf.set_text_color(0, 0, 0)  # Reset to black
                pdf.ln(10)
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
