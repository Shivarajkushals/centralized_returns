import streamlit as st
import pandas as pd
import mysql.connector
import requests
import json
from datetime import datetime
import os
from fpdf import FPDF
import base64

# Set Page Title
st.set_page_config(page_title="Centralized_retuns", layout="wide")

# Hide Streamlit's menu and footer
hide_streamlit_style = """
    <style>
        #MainMenu {visibility: hidden;} /* Hides the three dots menu */
        footer {visibility: hidden;} /* Hides the footer */
        header {visibility: hidden;} /* Hides the header */
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Initialize session state variables
if "page" not in st.session_state:
    st.session_state.page = "login"  # Default page is Login
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "Config" not in st.session_state:
    st.session_state.config = False
if "file_uploaded" not in st.session_state:
    st.session_state.file_uploaded = False


# Function to switch pages instantly
def switch_page(page_name):
    st.session_state.page = page_name
    st.rerun()  # Forces Streamlit to refresh the UI instantly

main_container = st.empty()

# Function to fetch and load credentials from GitHub
@st.cache_data
def load_credentials():
    url = "https://raw.githubusercontent.com/ShivarajMBB/Streamlit-repo/master/Security.txt"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            credentials = json.loads(response.text)  # Parse JSON content
            return {list(user.keys())[0]: list(user.values())[0] for user in credentials}  # Convert to dictionary
        else:
            st.error("Failed to load credentials. Please check your connection.")
            return {}
    except Exception as e:
        st.error(f"Error loading credentials: {str(e)}")
        return {}

# Load credentials
VALID_CREDENTIALS = load_credentials()

DB_CONFIG = st.secrets["db_config"]

def fetch_all_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Fetch full table data
        cursor.execute("SELECT * FROM tbl_wh_sales_returns;")
        sales_returns_data = cursor.fetchall()

        cursor.execute("SELECT * FROM tbl_wh_transfer_out;")
        transfer_out_data = cursor.fetchall()

        # # Fetch most recent records based on created_date
        # cursor.execute("""
        #     SELECT * FROM tbl_wh_sales_returns 
        #     WHERE created_date = (SELECT MAX(created_date) FROM tbl_wh_sales_returns);
        # """)
        # recent_sales_returns = cursor.fetchall()

        # cursor.execute("""
        #     SELECT * FROM tbl_wh_transfer_out 
        #     WHERE created_date = (SELECT MAX(created_date) FROM tbl_wh_transfer_out);
        # """)
        # recent_transfer_out = cursor.fetchall()

        # Fetch max SR number per store
        query = """
        SELECT store_name, max_sr
        FROM tbl_wh_store_config
        GROUP BY store_name;
        """
        cursor.execute(query)
        sr_numbers = {row["store_name"]: int(row["max_sr"][2:]) for row in cursor.fetchall() if row["max_sr"]}

        # Fetch store name mapping (lowercase to original case)
        cursor.execute("SELECT store_name FROM tbl_wh_store_config;")
        store_case_mapping = {row["store_name"].lower(): row["store_name"] for row in cursor.fetchall()}

        # Fetch max TO number per store
        cursor.execute("SELECT store_name, max_to FROM tbl_wh_store_config GROUP BY store_name;")
        to_numbers = {row["store_name"]: int(row["max_to"][2:]) for row in cursor.fetchall() if row["max_to"]}

        # Fetch max IDs from both tables
        cursor.execute("SELECT MAX(id) AS max_sr_id FROM tbl_wh_sales_returns;")
        max_sr_id = cursor.fetchone()["max_sr_id"] or 0

        cursor.execute("SELECT MAX(id) AS max_to_id FROM tbl_wh_transfer_out;")
        max_to_id = cursor.fetchone()["max_to_id"] or 0

        cursor.close()
        conn.close()

        return {
            "sales_returns_df": pd.DataFrame(sales_returns_data),
            "transfer_out_df": pd.DataFrame(transfer_out_data),
            # "recent_sales_returns_df": pd.DataFrame(recent_sales_returns),
            # "recent_transfer_out_df": pd.DataFrame(recent_transfer_out),
            "sr_numbers": sr_numbers,
            "to_numbers": to_numbers,
            "max_sr_id": max_sr_id,
            "max_to_id": max_to_id,
            "store_case_mapping": store_case_mapping
        }

    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")
        return {
            "sales_returns_df": pd.DataFrame(),
            "transfer_out_df": pd.DataFrame(),
            # "recent_sales_returns_df": pd.DataFrame(),
            # "recent_transfer_out_df": pd.DataFrame(),
            "sr_numbers": {},
            "to_numbers": {},
            "max_sr_id": 0,
            "max_to_id": 0,
            "store_case_mapping": {}
        }

# Simple function to filter out inactive stores
def filter_inactive_stores(uploaded_df):
    try:
        # Connect to database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Fetch only active stores
        cursor.execute("SELECT store_name FROM tbl_wh_store_config WHERE config = 1")
        active_stores = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Create list of active store names (case insensitive)
        active_store_list = [store['store_name'].lower() for store in active_stores]
        
        # Filter the dataframe to keep only active stores
        before_count = len(uploaded_df)
        uploaded_df = uploaded_df[uploaded_df['stores'].str.lower().isin(active_store_list)]
        after_count = len(uploaded_df)
        
        # Show notification if stores were filtered out
        if before_count > after_count:
            filtered_count = before_count - after_count
            st.warning(f"⚠️ {filtered_count} records from inactive stores were removed from processing.")
        
        return uploaded_df
        
    except Exception as e:
        st.error(f"❌ Error filtering stores: {e}")
        return uploaded_df  # Return original DataFrame if there's an error

def check_duplicates(uploaded_df, db_df):
    if db_df.empty:
        return uploaded_df, pd.DataFrame()  # No duplicates if database is empty
    
    # Create a working copy to preserve original data
    working_df = uploaded_df.copy()

    # Define column mappings (ensuring consistency)
    column_mapping = {
        "return_date": "date",
        "outlet_name": "stores",
        "bill_no": "bill no",
        "design_no": "design numbers",
    }

    # Ensure mapped columns exist in both DataFrames
    for db_col, up_col in column_mapping.items():
        if db_col not in db_df.columns or up_col not in working_df.columns:
            st.error(f"❌ Missing column: {db_col} in database or {up_col} in uploaded file!")
            return uploaded_df, pd.DataFrame()

    # Standardize column names for comparison
    db_comparison = db_df.rename(columns=column_mapping).copy()
    upload_comparison = working_df.copy()

    # Ensure consistency for comparison: convert to string, strip whitespace, and lowercase
    for col in column_mapping.values():
        upload_comparison[col] = upload_comparison[col].astype(str).str.strip().str.lower()
        db_comparison[col] = db_comparison[col].astype(str).str.strip().str.lower()

    # Merge to find duplicates
    merged_df = upload_comparison.merge(
        db_comparison,
        how="inner",
        on=["date", "stores", "bill no", "design numbers"]
    )

    # Extract duplicate records from original uploaded_df
    duplicate_indices = upload_comparison[
        upload_comparison.set_index(["date", "stores", "bill no", "design numbers"]).index.isin(
            merged_df.set_index(["date", "stores", "bill no", "design numbers"]).index
        )
    ].index

    duplicate_records = uploaded_df.loc[duplicate_indices].copy()
    non_duplicate_df = uploaded_df.drop(duplicate_indices).copy()

    if not duplicate_records.empty:
        st.warning(f"⚠️ Found {len(duplicate_records)} duplicate records. These will not be processed.")

    return non_duplicate_df, duplicate_records

# Function to assign SR numbers and return max SR per store
def assign_sr_numbers(uploaded_df, sr_dict, store_case_mapping):
    uploaded_df = uploaded_df.copy()
    uploaded_df["sr_no"] = ""
    max_sr_dict = {}

    # Create a mapping of lowercase store names to their original names in the uploaded data
    upload_store_mapping = {store.lower(): store for store in uploaded_df["stores"].unique()}

    for store_lower in [s.lower() for s in uploaded_df["stores"].unique()]:
        # Get the original cased store name from the database, or from uploaded data
        original_store = store_case_mapping.get(store_lower, upload_store_mapping.get(store_lower))
        
        # Get the matching original cased store name in the uploaded data
        upload_store = upload_store_mapping.get(store_lower)
        
        # Determine the last SR number for this store (case-insensitive lookup)
        last_sr = 0
        for db_store, sr_num in sr_dict.items():
            if db_store.lower() == store_lower:
                last_sr = sr_num
                break
                
        # Assign new SR numbers
        store_rows = uploaded_df["stores"].str.lower() == store_lower
        new_srs = [f"SR{str(last_sr + i + 1).zfill(3)}" for i in range(store_rows.sum())]
        uploaded_df.loc[store_rows, "sr_no"] = new_srs
        
        # Store the max SR for each store (with proper case)
        max_sr_dict[original_store] = f"SR{str(last_sr + store_rows.sum()).zfill(3)}"
        
        # Update the store name in the dataframe to maintain consistent case
        if original_store and original_store != upload_store:
            uploaded_df.loc[uploaded_df["stores"].str.lower() == store_lower, "stores"] = original_store

    return uploaded_df, max_sr_dict

# Function to assign TO numbers and return max TO per store
def assign_to_numbers(uploaded_df, to_dict, store_case_mapping):
    uploaded_df = uploaded_df.copy()
    uploaded_df["to_no"] = ""  
    max_to_dict = {}

    # Create a mapping of lowercase store names to their original names in the uploaded data
    upload_store_mapping = {store.lower(): store for store in uploaded_df["stores"].unique()}

    for store_lower in [s.lower() for s in uploaded_df["stores"].unique()]:
        # Get the original cased store name from the database, or from uploaded data
        original_store = store_case_mapping.get(store_lower, upload_store_mapping.get(store_lower))
        
        # Determine the last TO number for this store (case-insensitive lookup)
        last_to = 0
        for db_store, to_num in to_dict.items():
            if db_store.lower() == store_lower:
                last_to = to_num
                break
                
        # Assign new TO number
        new_to = f"TO{str(last_to + 1).zfill(3)}"
        uploaded_df.loc[uploaded_df["stores"].str.lower() == store_lower, "to_no"] = new_to
        
        # Store the max TO for each store (with proper case)
        max_to_dict[original_store] = new_to

    return uploaded_df, max_to_dict

# Function to assign incremental IDs
def assign_incremental_ids(uploaded_df, max_sr_id, max_to_id):
    uploaded_df = uploaded_df.copy()
    uploaded_df["sales_return_id"] = range(max_sr_id + 1, max_sr_id + 1 + len(uploaded_df))
    uploaded_df["transfer_out_id"] = range(max_to_id + 1, max_to_id + 1 + len(uploaded_df))
    return uploaded_df

def update_store_max_sr_to(DB_CONFIG, max_sr_dict, max_to_dict):
    try:
        # Connect to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Get existing store data
        cursor.execute("SELECT store_name, max_sr, max_to FROM tbl_wh_store_config")
        existing_data = {row[0]: {'max_sr': row[1], 'max_to': row[2]} for row in cursor.fetchall()}

        # Prepare update statements
        for store in set(max_sr_dict) | set(max_to_dict):
            new_sr = max_sr_dict.get(store)
            new_to = max_to_dict.get(store)

            # Case-insensitive store check
            existing_store = next((db_store for db_store in existing_data if db_store.lower() == store.lower()), None)

            if existing_store:
                update_query = """
                UPDATE tbl_wh_store_config 
                SET max_sr = %s, max_to = %s
                WHERE store_name = %s
                """
                cursor.execute(update_query, (new_sr, new_to, existing_store))
            else:
                insert_query = """
                INSERT INTO tbl_wh_store_config (store_name, max_sr, max_to)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (store, new_sr, new_to))

        # Commit updates
        conn.commit()

        print("✅ Store max SR and TO numbers updated successfully!")
        st.success("✅ Store max SR and TO numbers updated successfully!")

    except mysql.connector.Error as err:
        print("❌ Error:", err)
        st.error(f"❌ Error updating store config: {err}")

def fetch_sales_data(DB_CONFIG, start_date, end_date, selected_stores):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create placeholder string for IN clause
        store_placeholders = ','.join(['%s'] * len(selected_stores))

        # Updated sales_query_1 with filters
        sales_query_1 = f"""
        SELECT 
            t1.design_no, 
            t1.outlet_name, 
            t2.item_name, 
            t2.color, 
            t2.polish, 
            t2.size, 
            SUM(t1.sold_qty) AS Qty,
            SUM(t1.bill_amount) AS MRP_Amount,
            t3.address,
            t4.transfer_out_date,
            t4.transfer_out_no,
            SUM(t1.discount_amount) AS bill_discount
        FROM tbl_wh_sales_returns t1
        LEFT JOIN tbl_item_data t2 
            ON t1.combination_id = t2.combination_id
        LEFT JOIN tbl_wh_store_config t3
            ON t1.outlet_name = t3.store_name
        LEFT JOIN tbl_wh_transfer_out t4
            ON t1.id = t4.sr_id
        WHERE t1.return_date BETWEEN %s AND %s
        AND t1.outlet_name IN ({store_placeholders})
        GROUP BY 
            t1.design_no, 
            t1.outlet_name
        """

        params1 = [start_date, end_date] + selected_stores
        cursor.execute(sales_query_1, params1)
        sales_data_1 = cursor.fetchall()
        columns_1 = [desc[0] for desc in cursor.description]
        df_sales_1 = pd.DataFrame(sales_data_1, columns=columns_1)

        # Updated sales_query_2 with filters
        sales_query_2 = f"""
        SELECT 
            t1.design_no AS Design,  
            t2.item_name AS Product_name, 
            SUM(t1.sold_qty) AS Qty,
            SUM(t1.bill_amount) AS MRP_Amount,
            t1.sr_no
        FROM tbl_wh_sales_returns t1
        LEFT JOIN tbl_item_data t2 
            ON t1.combination_id = t2.combination_id
        WHERE t1.return_date BETWEEN %s AND %s
        AND t1.outlet_name IN ({store_placeholders})
        GROUP BY 
            t1.design_no
        """

        params2 = [start_date, end_date] + selected_stores
        cursor.execute(sales_query_2, params2)
        sales_data_2 = cursor.fetchall()
        columns_2 = [desc[0] for desc in cursor.description]
        df_sales_2 = pd.DataFrame(sales_data_2, columns=columns_2)

        cursor.close()
        conn.close()

        return df_sales_1, df_sales_2

    except mysql.connector.Error as err:
        print("❌ Error fetching sales data:", err)
        return None, None

def call_update_sales_returns():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.callproc("UpdateSalesReturns")
        conn.commit()
        cursor.close()
        conn.close()
        st.success("✅ Stored procedure 'UpdateSalesReturns' executed successfully.")
    except Exception as e:
        st.error(f"❌ Error calling stored procedure: {e}")

# Function to calculate Qty based on "-" in Design Numbers
def calculate_qty(design_number):
    return str(design_number).count("-") + 1 if pd.notna(design_number) else 1

def insert_data(df, table_name):
    """
    Inserts data from a DataFrame into a specified MySQL table.

    Parameters:
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the target table.

    Returns:
        int: Number of rows inserted, or 0 if an error occurs.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        values = [tuple(row) for row in df.to_numpy()]
        
        cursor.executemany(query, values)
        conn.commit()
        
        rows_affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        return rows_affected
        
    except Exception as e:
        st.error(f"❌ Error inserting data into {table_name}: {e}")
        return 0

# Function to split and expand rows
def expand_design_numbers(df):
    new_rows = []
    
    for _, row in df.iterrows():
        design_str = str(row["design numbers"])  # Ensure it's a string
        design_parts = design_str.split("-")  # Split by '-'
        
        # Only add split values (remove original row if it had "-")
        for part in design_parts:
            new_row = row.copy()
            new_row["design numbers"] = part.strip()  # Assign the new design number
            new_rows.append(new_row.to_dict())
    
    return pd.DataFrame(new_rows)

def generate_pdfs_from_df(df, output_folder="pdf_reports"):
    os.makedirs(output_folder, exist_ok=True)
    
    # Group by outlet_name and ensure each has its corresponding address
    outlet_groups = df.groupby(['outlet_name', 'address', "transfer_out_date", "transfer_out_no"])

    pdf_files = []

    for (outlet, address, date, no), outlet_df in outlet_groups:
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()

        def add_page_header():
            """ Function to add the header section for each page """
            pdf.set_xy(10, 1)
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(200, 8, "DELIVERY CHALLAN / STOCK TRANSFER OUT", ln=True, align="C")

            pdf.set_xy(80, 6.5)
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 8, "KUSHAL'S RETAIL PVT. LTD.", ln=False)

            pdf.set_xy(170, 6.5)
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(0, 8, "ORIGINAL", ln=True)

            pdf.rect(1, 13, 208, 16)  # Rectangle for company info
            pdf.ln(5)
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_xy(6, 22.8)  
            pdf.cell(0, 6, f"From: {outlet}", ln=True)

            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_xy(30, 15)  
            pdf.cell(0, 6, f"{address}", ln=True)  

            pdf.set_xy(92.5, 18.8)  
            pdf.cell(0, 6, "Tel: 8035276599", ln=True)  

            pdf.set_xy(85, 22.8)  
            pdf.cell(0, 6, "GSTIN: 29AAHCK5046J1Z6", ln=True)  

            pdf.rect(1, 29, 208, 26.5)  # Rectangle for destination info

            pdf.ln(18)
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_xy(6, 30)  
            pdf.cell(0, 6, "To: KUSHAL'S RETAIL PVT.LTD.       BLR WAREHOUSE - WH", ln=True)
            
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_xy(15, 36)  
            pdf.cell(0, 6, "No 19/3,1st and 2nd Floor, Bikasipura Main Road,", ln=True)  

            pdf.set_xy(15, 40)  
            pdf.cell(0, 6, "9th MAIN,3rd Block, Jayanagar, , BANGALORE -560011", ln=True)  

            pdf.set_xy(15, 44)  
            pdf.cell(0, 6, "Tel: 8035276599", ln=True)  

            pdf.set_xy(15, 48)  
            pdf.cell(0, 6, "GSTIN: 29AAHCK5046J1Z6", ln=True) 
            
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_xy(90, 50)  
            pdf.cell(0, 6, "Karnataka", ln=True) 
            
            pdf.set_font("Helvetica", "", 9)
            pdf.set_xy(120, 35)  
            pdf.cell(0, 6, f"Stock Trans ref No :       {no}", ln=True) 

            pdf.set_font("Helvetica", "", 9)
            pdf.set_xy(120, 40)  
            pdf.cell(0, 6, f"Date :                              {date}", ln=True) 
            
            pdf.rect(1, 55.5, 208, 237)  # Table border
            add_table_header()

        def add_table_header():
            """ Function to add table headers """
            pdf.set_xy(5, 57)
            pdf.set_font("Helvetica", "B", 9)

            # Define header positions
            header_positions = [3, 17, 72, 85, 105, 130, 150, 168, 180]
            headers = ["Sr No", "Item Description", "HSN", "Design No", "Color", "Polish", "Size", "Qty", "MRP Amount"]

            for x_pos, header in zip(header_positions, headers):
                pdf.set_x(x_pos)  
                pdf.cell(0, 6, header, align="L")
            pdf.ln(5)

        add_page_header()
        
        # **Header separator**
        pdf.rect(1, 55.5, 208, 6.8)  # (x, y, width, height)

        pdf.set_font("Helvetica", "", 9.2)
        row_spacing = 6
        outlet_df = outlet_df.reset_index(drop=True)

        total_qty = 0
        total_mrp = 0.0
        total_dis = 0.0
        net_total = 0.0

        for i, row in outlet_df.iterrows():
            if pdf.get_y() > 250:  # Ensure enough space for totals
                pdf.add_page()
                add_page_header()

            pdf.set_x(3)
            pdf.cell(14, row_spacing, str(i + 1), align="L")  # Sr No

            pdf.set_x(17)
            pdf.cell(55, row_spacing, str(row['item_name']), align="L")  # Item Description

            pdf.set_x(72)
            pdf.cell(13, row_spacing, "7117", align="L")  # HSN (Constant Value)

            pdf.set_x(85)
            pdf.cell(20, row_spacing, str(row['design_no']), align="L")  # Design No

            pdf.set_x(105)
            pdf.cell(25, row_spacing, str(row['color']), align="L")  # Color

            pdf.set_x(130)
            pdf.cell(20, row_spacing, str(row['polish']), align="L")  # Polish

            pdf.set_x(150)
            pdf.cell(18, row_spacing, str(row['size']), align="L")  # Size

            pdf.set_x(166)
            pdf.cell(12, row_spacing, str(row['Qty']), align="C")  # Qty
            total_qty += int(row['Qty'])

            pdf.set_x(172)
            pdf.cell(28, row_spacing, str(row['MRP_Amount']), align="R")  # MRP Amount
            total_mrp += float(row['MRP_Amount'])


            pdf.ln(row_spacing)
            
            total_dis += float(row['bill_discount'])
            net_total = total_mrp + total_dis
# =============================================================================
#         # Ensure enough space for totals
#         if pdf.get_y() > 250:
#             pdf.add_page()
#             add_page_header()
# =============================================================================

        # Ensure totals are positioned exactly 40 units from the bottom
        y_position_for_totals = 287 - 45  # Page height is 297, keep totals at 40 from bottom
        
        if pdf.get_y() > y_position_for_totals:
            pdf.add_page()
            add_page_header()
        
        pdf.rect(1, 240, 208, 52.4)  # Box for totals and notes
        
        # Move to the correct position for totals
        pdf.set_xy(3, y_position_for_totals)
        pdf.set_font("Helvetica", "B", 10)
        
        # Draw Total Qty & Total Amount on the same line
        pdf.cell(130, row_spacing, "Total Qty", align="R")
        pdf.cell(12, row_spacing, str(total_qty), align="C")  # Total Quantity
        
        # Set X position for Total Amount
        pdf.set_x(150)  
        pdf.cell(30, row_spacing, "Total Amount", align="R")
        pdf.cell(20, row_spacing, str(total_mrp), align="R")  # Total MRP Amount
        
        pdf.ln(8)  # Move down for HSN
        
        # Set position for HSN
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(10, 242  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "HSN", align="L")
        
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(10, 250  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "7117", align="L")
        
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(35, 242  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "GST", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(35, 250  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "0", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(60, 242  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "Qty", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(60, 250  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "A", align="L")
        
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(80, 242  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "Amount", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(80, 250  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, "B", align="L")
        
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(156, 250)  # Adjust X position as needed
        pdf.cell(0, row_spacing, "Discount", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(187.2, 250  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, str(total_dis), align="L")
        
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(156, 256)  # Adjust X position as needed
        pdf.cell(0, row_spacing, "Net Total", align="L")

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_xy(187.2, 256  )  # Adjust X position as needed
        pdf.cell(0, row_spacing, str(net_total), align="L")
        
        pdf.ln(10)  # Move down after HSN


        # Ensure the note is positioned at the bottom
        pdf.set_xy(60, 287 - 11.4)  # 10 units from the bottom of the page
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 6, "Note: This is a system-generated document. No signature is required.", align="L")
        



        pdf_filename = os.path.join(output_folder, f"{outlet}.pdf")
        pdf.output(pdf_filename)
        pdf_files.append(pdf_filename)

    return pdf_files

def generate_sales_return_pdfs(df_sales_2):
    """
    Generate receipt-sized sales return PDFs for each design number in the sales data.
    
    Args:
        sales_data (pd.DataFrame): DataFrame containing sales return data
    
    Returns:
        dict: Dictionary mapping design numbers to PDF downloads
    """
    if df_sales_2 is None or df_sales_2.empty:
        st.error("No sales data available to generate PDFs.")
        return {}
    
    pdf_downloads = {}
    
    # Group the data by Design number
    grouped_data = df_sales_2.groupby('sr_no')
    
    for sr_no, group in grouped_data:
        try:
            # Create PDF with receipt dimensions (80mm width is common for receipts)
            # 80mm = approx 3.15 inches = 226.8 points (1 inch = 72 points in FPDF)
            pdf = FPDF(orientation='P', unit='mm', format=(80, 200))  # 80mm width, variable height
            pdf.set_auto_page_break(True, margin=10)
            pdf.add_page()
            pdf.set_font("Arial", size=8)  # Smaller font for receipt
            
            # Set margins for receipt
            pdf.set_margins(5, 5, 5)  # left, top, right margins in mm
            
            # Header - smaller for receipt
            pdf.set_font("Arial", 'B', 10)
            pdf.cell(0, 5, "KUSHAL'S FASHION JEWELLERY", ln=True, align='C')
            pdf.set_font("Arial", size=7)
            pdf.cell(0, 3, "NO. 6, SHAMBU TOWERS, D.V.G ROAD", ln=True, align='C')
            pdf.cell(0, 3, "GANDHI BAZAR, BASAVANAGUDI", ln=True, align='C')
            pdf.cell(0, 3, "BANGALORE, KARNATAKA", ln=True, align='C')
            pdf.cell(0, 3, "PH No. : 08042108611", ln=True, align='C')
            pdf.cell(0, 3, "www.kushals.com", ln=True, align='C')
            
            # Sales Return Title
            pdf.set_font("Arial", 'B', 9)
            pdf.cell(0, 5, "SALES RETURN", ln=True, align='C')
            
            # SR Bill No and Date
            current_date = datetime.now().strftime("%d/%m/%Y")
            current_time = datetime.now().strftime("%H:%M:%S")
            sr_bill_no = f"BGR{sr_no}{datetime.now().strftime('%y-%y')}"
            
            pdf.set_font("Arial", size=7)
            pdf.cell(40, 3, f"SR Bill No: {sr_bill_no}", 0, 0)
            pdf.cell(30, 3, f"Date: {current_date}", 0, 1, 'R')
            pdf.cell(0, 3, f"Time: {current_time}", 0, 1, 'R')
            
            # Divider
            pdf.cell(0, 1, "-" * 1200, ln=True, align='C')
            
            # Table Header - compact for receipt
            pdf.set_font("Arial", 'B', 7)
            pdf.cell(5, 4, "#", 0, 0)
            pdf.cell(25, 4, "PRODUCT NAME", 0, 0)
            pdf.cell(15, 4, "ARTICLE", 0, 0)
            pdf.cell(7, 4, "Qty", 0, 0)
            pdf.cell(10, 4, "Rate", 0, 0)
            pdf.cell(13, 4, "Amount", 0, 1)
            pdf.cell(5, 3, "", 0, 0)
            pdf.cell(25, 3, "HSN", 0, 0)
            pdf.cell(40, 3, "DESIGN", 0, 1)
            
            # Divider
            pdf.cell(0, 1, "-" * 120, ln=True, align='C')
            
            # Table Data
            total_amount = 0
            for idx, row in group.iterrows():
                pdf.set_font("Arial", size=7)
                
                # Product details
                pdf.cell(5, 4, str(idx + 1), 0, 0)
                pdf.cell(25, 4, str(row['Product_name']), 0, 0)
                
                # Article number (using sr_no or creating a placeholder)
                article_no = str(row.get('sr_no', f"0000010{row['Design']}"))
                pdf.cell(15, 4, article_no, 0, 0)
                
                # Quantity, Rate, Amount
                qty = int(row['Qty'])
                amount = float(row['MRP_Amount'])
                rate = amount / qty if qty > 0 else 0
                
                pdf.cell(7, 4, str(qty), 0, 0)
                pdf.cell(10, 4, f"{rate:.2f}", 0, 0)
                pdf.cell(13, 4, f"{amount:.2f}", 0, 1)
                
                # HSN and Design
                pdf.cell(5, 3, "", 0, 0)
                pdf.cell(25, 3, "7117", 0, 0)  # Placeholder HSN code
                pdf.cell(40, 3, str(row['Design']), 0, 1)
                
                total_amount += amount
            
            # Divider
            pdf.cell(0, 1, "-" * 120, ln=True, align='C')
            
            # Total
            pdf.set_font("Arial", 'B', 7)
            pdf.cell(52, 4, "TOTAL", 0, 0)
            pdf.cell(7, 4, str(group['Qty'].sum()), 0, 0)
            pdf.cell(13, 4, f"{total_amount:.2f}", 0, 1, 'R')
            
            # Divider
            pdf.cell(0, 1, "-" * 120, ln=True, align='C')
            
            # Tax and Net Amount
            tax_amount = total_amount * 0.03  # Assuming 3% tax
            cgst = tax_amount / 2
            sgst = tax_amount / 2
            
            pdf.cell(52, 4, "TAX AMT :", 0, 0)
            pdf.cell(18, 4, f"{tax_amount:.2f}", 0, 1, 'R')
            
            pdf.set_font("Arial", 'B', 7)
            pdf.cell(52, 4, "NET AMT :", 0, 0)
            pdf.cell(18, 4, f"{total_amount:.2f}", 0, 1, 'R')
            
            # Divider
            pdf.cell(0, 1, "-" * 120, ln=True, align='C')
            
            # CGST and SGST
            pdf.set_font("Arial", size=7)
            pdf.cell(15, 4, "CGST%", 0, 0)
            pdf.cell(15, 4, "CGST AMT", 0, 0)
            pdf.cell(15, 4, "SGST%", 0, 0)
            pdf.cell(15, 4, "SGST AMT", 0, 1)
            
            pdf.cell(15, 4, "1.50", 0, 0)
            pdf.cell(15, 4, f"{cgst:.2f}", 0, 0)
            pdf.cell(15, 4, "1.50", 0, 0)
            pdf.cell(15, 4, f"{sgst:.2f}", 0, 1)
            
            # GSTIN
            pdf.cell(0, 4, "GSTIN # : 29AAHCK5046J1Z6", 0, 1)
            
            # Terms and Conditions
            pdf.set_font("Arial", 'B', 7)
            pdf.cell(0, 4, "Terms And Conditions", 0, 1)
            pdf.set_font("Arial", size=6)
            pdf.cell(0, 3, "1. No Exchange, No Gaurantee, No Refund", 0, 1)
            pdf.cell(0, 3, "2. Prices inclusive of taxes, subject to statutory terms", 0, 1)
            pdf.cell(0, 3, "3. All disputes are subject to Bangalore jurisdiction", 0, 1)
            
            # Generate PDF
            pdf_output = f"sales_return_{sr_no}.pdf"
            pdf.output(pdf_output)
            
            # Create download button
            with open(pdf_output, "rb") as f:
                pdf_bytes = f.read()
            
            b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
            download_link = f'<a href="data:application/pdf;base64,{b64_pdf}" download="{pdf_output}">Download PDF for sr_no {sr_no}</a>'
            
            pdf_downloads[sr_no] = download_link
            
            # Clean up temporary file
            os.remove(pdf_output)
            
        except Exception as e:
            st.error(f"Error generating PDF for sr_no {sr_no}: {str(e)}")
    
    return pdf_downloads

def display_sales_return_pdfs(df_sales_2):
    st.header("Generated Sales Return PDFs")
    
    if df_sales_2 is None or df_sales_2.empty:
        st.warning("No sales data available to generate PDFs.")
        return
    
    # Debug information
    st.write(f"Number of records in sales_returns: {len(df_sales_2)}")
    # st.write(f"Number of unique design numbers: {df_sales_2['Design'].nunique()}")
    
    # Generate PDFs
    pdf_downloads = generate_sales_return_pdfs(df_sales_2)
    
    if not pdf_downloads:
        st.warning("No PDFs were generated.")
        return
    
    # Display download links
    st.write("Download Sales Return PDFs:")
    for sr_no, download_link in pdf_downloads.items():
        st.markdown(download_link, unsafe_allow_html=True)
# ---------------------------- PAGE 1: LOGIN ---------------------------------
if st.session_state.page == "login":
    with main_container.container():
        st.markdown(
            """
            <style>
                .sign-in-title { text-align: center; font-weight: bold; margin-bottom: -10px; }
                .sign-in-subtext { text-align: center; font-size: 14px; color: #6c757d; margin-top: -10px; }
                .stButton > button { width: 100%; padding: 12px; font-size: 18px; font-weight: bold;
                                     background-color: #007BFF; color: white; border: none; border-radius: 8px; cursor: pointer; }
                .stButton > button:hover { background-color: #0056b3; }
            </style>
            <h2 class='sign-in-title'>Sign In</h2>
            <p class='sign-in-subtext'>Enter your email and password to sign in</p>
            """,
            unsafe_allow_html=True
        )
    
        # Initialize spacing for logo
        if "logo_spacing" not in st.session_state:
            st.session_state.logo_spacing = 200

        st.session_state.logo_spacing = 200
        
        # Create columns for centered layout
        col1, col2, col3 = st.columns([1, 2, 1])  
        with col2:  
            inner_col1, inner_col2, inner_col3 = st.columns([0.8, 2, 0.8])
            with inner_col2:
                email = st.text_input("Email*", placeholder="mail@simmmple.com")
                password = st.text_input("Password*", placeholder="Min. 8 characters", type="password")
    
                # Functional Sign In Button
                if st.button("Sign In"):  
                    if email in VALID_CREDENTIALS and VALID_CREDENTIALS[email] == password:
                        st.session_state.authenticated = True
                        switch_page("Config")  # Move to Upload Page Immediately
                    else:
                        st.error("Invalid email or password!")
                        st.session_state.logo_spacing = 128  # Reduce spacing when error appears

            # Footer text
            st.markdown(
                """
                <style>
                    .rights-text { text-align: center; font-size: 8px; color: #6c757d; margin-top: 20px; }
                </style>
                <p class='rights-text'>© 2024 All Rights Reserved. Made with love by Technoboost !</p>
                """,
                unsafe_allow_html=True
            )

elif st.session_state.page == "Config":
    
    # Backup credentials before clearing cache
    VALID_CREDENTIALS = load_credentials()
    st.session_state["valid_credentials"] = VALID_CREDENTIALS  # Store in session state
    
    # Clear all cache except credentials
    st.cache_data.clear()
    
    # Restore cached credentials
    VALID_CREDENTIALS = st.session_state["valid_credentials"]
    
    # Ensure authentication check
    if not st.session_state.authenticated:
        switch_page("login")
        
    st.title("Store Configuration")
    
    # Function to fetch store config data
    @st.cache_data(ttl=10)  # Cache for 10 seconds to allow refreshing after updates
    def fetch_store_config():
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("SELECT store_name, config FROM tbl_wh_store_config ORDER BY store_name;")
            stores_data = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return pd.DataFrame(stores_data)
        except Exception as e:
            st.error(f"❌ Error fetching store configuration: {e}")
            return pd.DataFrame()
    
    # Function to update store configuration
    def update_store_config(store_name, active_status):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            update_query = """
            UPDATE tbl_wh_store_config 
            SET config = %s
            WHERE store_name = %s
            """
            cursor.execute(update_query, (active_status, store_name))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
        except Exception as e:
            st.error(f"❌ Error updating store configuration: {e}")
            return False
    
    # Get store configuration data
    stores_df = fetch_store_config()
    
    if not stores_df.empty:
        # Check if 'is_active' column exists, if not create it
        if 'config' not in stores_df.columns:
            stores_df['config'] = None
        
        # Create two columns: one for the store list and one for the configuration form
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Current Store Configuration")
            # Show the dataframe with store configuration
            st.dataframe(stores_df, use_container_width=True)
        
        with col2:
            st.subheader("Update Store Status")
            # Create a form for updating store configuration
            with st.form("store_config_form"):
                # Create a dropdown with store names
                store_names = stores_df['store_name'].tolist()
                selected_store = st.selectbox("Select Store", store_names)
                
                # Get current status for the selected store
                current_status = None
                if 'config' in stores_df.columns:
                    selected_row = stores_df[stores_df['store_name'] == selected_store]
                    if not selected_row.empty and pd.notna(selected_row['config'].iloc[0]):
                        current_status = bool(selected_row['config'].iloc[0])
                
                # Status selection (defaulting to current value if it exists)
                status_options = {"Active": 1, "Inactive": 0}
                default_index = 0 if current_status in (None, True) else 1
                status = st.radio("Store Status", list(status_options.keys()), index=default_index)
                
                # Submit button
                submit_button = st.form_submit_button("Update Configuration")
                
                if submit_button:
                    active_status = status_options[status]
                    success = update_store_config(selected_store, active_status)
                    
                    if success:
                        st.success(f"✅ Successfully updated {selected_store} to {status}")
                        # Clear the cache to refresh the data
                        fetch_store_config.clear()
                        # Rerun to show updated data
                        st.rerun()
    else:
        st.warning("⚠️ No store configuration data available. Please check the database connection.")
        
    if st.button("Continue"):
        switch_page("upload")
                                  
# =============================================================================
#         # Centering the logo
#         with col2:
#             inner_col1, inner_col2, inner_col3 = st.columns([0.85, 0.5, 0.85])  
#             with inner_col2:
#                 st.markdown(f"<div style='height: {st.session_state.logo_spacing}px;'></div>", unsafe_allow_html=True)
#                 st.image("https://raw.githubusercontent.com/ShivarajMBB/Streamlit-repo/master/Kushals_logo.jpg", width=125)
# =============================================================================
    
# Add this inside your application code after your current page options
elif st.session_state.page == "upload":    
    page = st.sidebar.radio("Select Page", ["Upload page", "SR page", "TO page"])  # Added "Config page"
    
    # Existing page code remains unchanged
    if page == "Upload page":
        col1, col2, col3 = st.columns([1.5, 8, 1.5])
        with col2:
            # Streamlit UI
            st.title("Upload the excel file to generate SR and TO files")
            
            uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx"])
            
            if uploaded_file is not None:
                if uploaded_file.name.endswith(".csv"):
                    uploaded_df = pd.read_csv(uploaded_file)
                else:
                    try:
                        uploaded_df = pd.read_excel(uploaded_file, engine="openpyxl")
                    except ImportError:
                        st.error("Missing dependency: Please install 'openpyxl' using `pip install openpyxl`.")
                
                st.write("Uploaded file")
                st.dataframe(uploaded_df)
            
                # Standardizing column names (Fix for 'Stores' KeyError)
                uploaded_df.columns = uploaded_df.columns.str.strip().str.lower()
                
                uploaded_df = expand_design_numbers(uploaded_df)
                
                uploaded_df["qty"] = uploaded_df["design numbers"].astype(str).apply(calculate_qty)
                
                # Filter out inactive stores
                uploaded_df = filter_inactive_stores(uploaded_df)
            
                st.dataframe(uploaded_df)
            
                data = fetch_all_data()  # Fetch all required data in one go
                
                db_df = data["sales_returns_df"]  # tbl_wh_sales_returns data
                db_transfer_out = data["transfer_out_df"]  # tbl_wh_transfer_out data
                
                sr_dict = data["sr_numbers"]  # Max SR numbers per store
                to_dict = data["to_numbers"]  # Max TO numbers per store
                store_case_mapping = data["store_case_mapping"]  # Store name case mapping
                
                max_sr_id = data["max_sr_id"]  # Max ID from tbl_wh_sales_returns
                max_to_id = data["max_to_id"]  # Max ID from tbl_wh_transfer_out
            
                uploaded_df, duplicate_records = check_duplicates(uploaded_df, db_df)
                
                # Use the modified functions with store_case_mapping
                uploaded_df, max_sr_dict = assign_sr_numbers(uploaded_df, sr_dict, store_case_mapping)
                uploaded_df, max_to_dict = assign_to_numbers(uploaded_df, to_dict, store_case_mapping)
            
                if not duplicate_records.empty:
                    st.warning("⚠️ Duplicate records found! These will not be processed.")
                    st.dataframe(duplicate_records)
                    
                    csv_duplicates = duplicate_records.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Duplicate Records", csv_duplicates, "duplicate_records.csv", "text/csv")
            
            
                if not uploaded_df.empty:
                    st.success("✅ Processing non-duplicate data...")
            
                    uploaded_df = assign_incremental_ids(uploaded_df, max_sr_id, max_to_id)
            
                    required_columns = ["sales_return_id", "stores", "bill no", "design numbers", "qty", "date", "sr_no", "sr amount", "invoice no", "order no", "tender", "combination_id"]
                    
                    for col in required_columns:
                        if col not in uploaded_df.columns:
                            st.error(f"❌ Missing required column: {col}")
                            st.stop()
                            
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    sr_df = uploaded_df[required_columns].copy()
                    sr_df.rename(columns={
                        "sales_return_id": "id",
                        "sr amount": "bill_amount",
                        "invoice no": "invoice_no",
                        "order no": "order_no",
                        "stores": "outlet_name", 
                        "bill no": "bill_no", 
                        "design numbers": "design_no", 
                        "qty": "Sold_qty", 
                        "date": "return_date"
                    }, inplace=True)
                    
                    # Adding additional constant columns
                    sr_df["is_active"] = 1
                    sr_df["created_date"] = current_time  # Convert to string
                    sr_df["modified_date"] = current_time  
                    sr_df["return_date"] = pd.to_datetime(sr_df["return_date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    sr_df["created_by"] = "WH Team"
                    sr_df["modified_by"] = "WH Team"  
                    sr_df["tran_type"] = "Sales Returns" 
            
                    to_df = uploaded_df[["transfer_out_id", "stores", "to_no", "qty","sales_return_id", "date"]].copy()
                    to_df.rename(columns={"stores": "outlet_name_from",
                                          "to_no": "transfer_out_no",
                                          "sales_return_id": "sr_id",
                                          "transfer_out_id": "id",
                                          "date": "return_date"}, inplace=True)
                    
                    # Adding additional constant columns
                    to_df["is_active"] = 1  
                    to_df["created_date"] = current_time  # Convert to string
                    to_df["modified_date"] = current_time 
                    to_df["created_by"] = "WH Team"
                    to_df["return_date"] = pd.to_datetime(to_df["return_date"]).dt.strftime('%Y-%m-%d %H:%M:%S')  
                    to_df["modified_by"] = "WH Team"  
                    to_df["branch_recived"] = "Banglore_WH" 
                    to_df["transfer_out_date"] = current_time
            
                    rows_inserted = insert_data(sr_df, "tbl_wh_sales_returns")
                    rows_inserted = insert_data(to_df, "tbl_wh_transfer_out")
                    call_update_sales_returns()
                    sr_to_max = update_store_max_sr_to(DB_CONFIG, max_sr_dict, max_to_dict)
            
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_sales_returns.")
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_transfer_out.")
                    
                    csv_uploaded = uploaded_df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Updated CSV", csv_uploaded, "updated_data.csv", "text/csv")
            
                else:
                    st.warning("⚠️ No new data to process after removing duplicates.")
        pass
    
    elif page == "SR page":
        st.subheader("🔍 Filter Sales returns Data")

        # UI filters
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")

        # Fetch distinct store names for dropdown
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT outlet_name FROM tbl_wh_sales_returns;")
        store_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        selected_stores = st.multiselect("Select Store(s)", store_names)

        if st.button("✅ Continue"):
            if not selected_stores:
                st.warning("Please select at least one store.")
            else:
                try:
                    conn = mysql.connector.connect(**DB_CONFIG)
                    cursor = conn.cursor(dictionary=True)

                    # Format the placeholders and store list
                    store_placeholders = ','.join(['%s'] * len(selected_stores))

                    query = f"""
                        SELECT * FROM tbl_wh_sales_returns
                        WHERE return_date BETWEEN %s AND %s
                        AND outlet_name IN ({store_placeholders})
                    """

                    params = [start_date, end_date] + selected_stores
                    cursor.execute(query, params)
                    filtered_data = cursor.fetchall()

                    df_filtered = pd.DataFrame(filtered_data)
                    cursor.close()
                    conn.close()

                    if df_filtered.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Filtered data from `tbl_wh_sales_returns`:")
                        st.dataframe(df_filtered.reset_index(drop=True))
                except Exception as e:
                    st.error(f"❌ Error querying data: {e}")

            # data = fetch_all_data()  # Fetch all required data in one go
            
            # st.write("Data from database table tbl_wh_sales_returns:")
            # db_df = data["sales_returns_df"]
            # st.dataframe(db_df.reset_index(drop=True))
                    
            # st.write("Recently uploaded data:")
            # recent_df = data["recent_sales_returns_df"]
            # st.dataframe(recent_df)
            
            _, df_sales_2 = fetch_sales_data(DB_CONFIG, start_date, end_date, selected_stores)
            to_display = pd.DataFrame(df_sales_2)

            st.write("SR PDF output:")
            display_df = to_display
            st.dataframe(display_df)

            # =============================================================================
            # st.header("Generated Sales Reports")
            # =============================================================================

            if df_sales_2 is not None and not df_sales_2.empty:
                display_sales_return_pdfs(df_sales_2)
            pass
    
    elif page == "TO page":
        st.subheader("🔍 Filter Transfer Out Data")

        # UI filters
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")

        # Fetch distinct store names for dropdown
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT outlet_name_from FROM tbl_wh_transfer_out;")
        store_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        selected_stores = st.multiselect("Select Store(s)", store_names)

        if st.button("✅ Continue"):
            if not selected_stores:
                st.warning("Please select at least one store.")
            else:
                try:
                    conn = mysql.connector.connect(**DB_CONFIG)
                    cursor = conn.cursor(dictionary=True)

                    # Format the placeholders and store list
                    store_placeholders = ','.join(['%s'] * len(selected_stores))

                    query = f"""
                        SELECT * FROM tbl_wh_transfer_out
                        WHERE return_date BETWEEN %s AND %s
                        AND outlet_name_from IN ({store_placeholders})
                    """

                    params = [start_date, end_date] + selected_stores
                    cursor.execute(query, params)
                    filtered_data = cursor.fetchall()

                    df_filtered = pd.DataFrame(filtered_data)
                    cursor.close()
                    conn.close()

                    if df_filtered.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Filtered data from `tbl_wh_transfer_out`:")
                        st.dataframe(df_filtered.reset_index(drop=True))
                except Exception as e:
                    st.error(f"❌ Error querying data: {e}")
            # data = fetch_all_data()  # Fetch all required data in one go
            df_sales_1, _ = fetch_sales_data(DB_CONFIG, start_date, end_date, selected_stores)
            to_display = pd.DataFrame(df_sales_1)
            
            # st.write("Data from database table tbl_wh_transfer_out:")
            # db_df = data["transfer_out_df"]
            # st.dataframe(db_df.reset_index(drop=True))
                    
            # st.write("Recently uploaded data:")
            # recent_df = data["recent_transfer_out_df"]
            # st.dataframe(recent_df)
            
            # Display sales data
            st.write("TO PDF output:")
            if isinstance(df_sales_1, pd.DataFrame) and not df_sales_1.empty:
                st.dataframe(df_sales_1)
        
                # Generate PDFs
                pdf_files = generate_pdfs_from_df(df_sales_1)
        
                # Streamlit UI for downloading PDFs
                st.header("Download Sales Reports")
                for pdf_file in pdf_files:
                    outlet_name = os.path.basename(pdf_file).replace(".pdf", "")  # Extract outlet name
                    with open(pdf_file, "rb") as f:
                        st.download_button(
                            label=f"📥 Download {outlet_name}.pdf",
                            data=f,
                            file_name=f"{outlet_name}.pdf",
                            mime="application/pdf"
                        )
            else:
                st.error("❌ No data available to generate PDFs.")
