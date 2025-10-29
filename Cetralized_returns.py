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
if "upload_page" not in st.session_state:
    st.session_state.upload_page = "RTV page"


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

        # Fetch max SR number per store
        query = """ 
        SELECT sr_no
        FROM tbl_wh_sales_returns
        WHERE id = (SELECT MAX(id) FROM tbl_wh_sales_returns);
        """
        cursor.execute(query)
        row = cursor.fetchone()
        sr_number = row["sr_no"] if row and "sr_no" in row else None

        # Fetch store name mapping (lowercase to original case)
        cursor.execute("SELECT store_name FROM tbl_wh_store_config;")
        store_case_mapping = {row["store_name"].lower(): row["store_name"] for row in cursor.fetchall()}

        # Fetch max TO number per store
        cursor.execute("SELECT store_name, max_to FROM tbl_wh_store_config GROUP BY store_name;")
        to_numbers = {row["store_name"]: int(row["max_to"][2:]) for row in cursor.fetchall() if row["max_to"]}

        # Fetch max batch number (whole number only)
        cursor.execute("SELECT MAX(batch_no) AS max_batch_no FROM tbl_wh_sales_returns;")
        batch_result = cursor.fetchone()
        max_batch_no = int(batch_result["max_batch_no"]) if batch_result["max_batch_no"] is not None else 0

        cursor.close()
        conn.close()

        return {
            "sales_returns_df": pd.DataFrame(sales_returns_data),
            "transfer_out_df": pd.DataFrame(transfer_out_data),
            "sr_numbers": sr_number,
            "to_numbers": to_numbers,
            "store_case_mapping": store_case_mapping,
            "next_batch_no": max_batch_no + 1
        }

    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")
        return {
            "sales_returns_df": pd.DataFrame(),
            "transfer_out_df": pd.DataFrame(),
            "sr_numbers": {},
            "to_numbers": {},
            "store_case_mapping": {},
            "next_batch_no": 1
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
        uploaded_df['stores_lower'] = uploaded_df['stores'].str.lower()
        inactive_df = uploaded_df[~uploaded_df['stores_lower'].isin(active_store_list)].drop(columns=['stores_lower'])
        
        # Filter the dataframe to keep only active stores
        before_count = len(uploaded_df)
        uploaded_df = uploaded_df[uploaded_df['stores'].str.lower().isin(active_store_list)]
        after_count = len(uploaded_df)
        
        # Show notification if stores were filtered out
        if before_count > after_count:
            filtered_count = before_count - after_count
            st.warning(f"⚠️ {filtered_count} records from inactive stores were removed from processing.")
        
        return uploaded_df, inactive_df
        
    except Exception as e:
        st.error(f"❌ Error filtering stores: {e}")
        return uploaded_df, pd.DataFrame()  # Return original DataFrame if there's an error

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
        "combination_id": "combination_id",
        "barcode": "barcode",
    }

    # Ensure mapped columns exist in both DataFrames
    for db_col, up_col in column_mapping.items():
        if db_col not in db_df.columns or up_col not in working_df.columns:
            st.error(f"❌ Missing column: {db_col} in database or {up_col} in uploaded file!")
            return uploaded_df, pd.DataFrame()

    # Standardize column names for comparison
    db_comparison = db_df.rename(columns=column_mapping).copy()
    upload_comparison = working_df.copy()

    # Clean and standardize columns
    for col in column_mapping.values():
        upload_comparison[col] = '"' + upload_comparison[col].astype(str).str.strip().str.lower() + '"'
        db_comparison[col] = '"' + db_comparison[col].astype(str).str.strip().str.lower() + '"'
        
        # Special handling for barcode: remove double quotes
        if col == "date":
            upload_comparison[col] = upload_comparison[col].str.replace(' 00:00:00', '', regex=False)

    # Merge to find duplicates
    merged_df = upload_comparison.merge(
        db_comparison,
        how="inner",
        on=["date", "stores", "bill no", "combination_id", "barcode"]
    )

    # Extract duplicate records from original uploaded_df
    duplicate_indices = upload_comparison[
        upload_comparison.set_index(["date", "stores", "bill no", "combination_id", "barcode"]).index.isin(
            merged_df.set_index(["date", "stores", "bill no", "combination_id", "barcode"]).index
        )
    ].index

    duplicate_records = uploaded_df.loc[duplicate_indices].copy()
    non_duplicate_df = uploaded_df.drop(duplicate_indices).copy()

    if not duplicate_records.empty:
        st.warning(f"⚠️ Found {len(duplicate_records)} duplicate records. These will not be processed.")

    return non_duplicate_df, duplicate_records

# Function to assign SR numbers and return max SR per store
def assign_sr_numbers(uploaded_df, sr_number):
    uploaded_df = uploaded_df.copy()
    uploaded_df["sr_no"] = ""

    global_max_sr = 0
    if isinstance(sr_number, str) and sr_number.startswith("SR"):
        try:
            sr_body = sr_number[2:]  # remove 'SR'
            sr_main = sr_body.split('/')[0]
            if sr_main.isdigit():
                global_max_sr = int(sr_main)
        except Exception as e:
            st.write(f"Skipping invalid SR number: {sr_number} — Error: {e}")

    # Now assign new SR numbers
    new_sr_list = []
    for i in range(len(uploaded_df)):
        new_sr = f"SR{str(global_max_sr + i + 1).zfill(3)}"
        new_sr_list.append(new_sr)

    uploaded_df["sr_no"] = new_sr_list
    return uploaded_df


# Function to assign TO numbers and return max TO per store
def assign_to_numbers(uploaded_df, to_dict, store_case_mapping):
    uploaded_df = uploaded_df.copy()
    uploaded_df["to_no"] = ""  
    max_to_dict = {}

    # Create a mapping of lowercase store names to their original names in the uploaded data
    upload_store_mapping = {store.lower(): store for store in uploaded_df["stores"].unique()}

    for store_lower in [s.lower() for s in uploaded_df["stores"].unique()]:
        # --- SKIP BLR - WAREHOUSE ---
        if store_lower == "blr - warehouse".lower():
            continue  # Skip assigning any to_no

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

def update_store_max_sr_to(DB_CONFIG, max_to_dict):
    try:
        # Connect to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Get existing store data
        cursor.execute("SELECT store_name, max_sr, max_to FROM tbl_wh_store_config")
        existing_data = {row[0]: {'max_sr': row[1], 'max_to': row[2]} for row in cursor.fetchall()}

        # Prepare update statements
        for store in set(max_to_dict):
            new_to = max_to_dict.get(store)

            # Case-insensitive store check
            existing_store = next((db_store for db_store in existing_data if db_store.lower() == store.lower()), None)

            if existing_store:
                update_query = """
                UPDATE tbl_wh_store_config 
                SET  max_to = %s
                WHERE store_name = %s
                """
                cursor.execute(update_query, ( new_to, existing_store))
            else:
                insert_query = """
                INSERT INTO tbl_wh_store_config (store_name, max_to)
                VALUES (%s, %s)
                """
                cursor.execute(insert_query, (store, new_to))

        # Commit updates
        conn.commit()

        st.success("✅ Store max SR and TO numbers updated successfully!")

    except mysql.connector.Error as err:
        st.write("❌ Error:", err)
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
        WHERE date(t1.created_date) BETWEEN %s AND %s
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
            t1.sr_no,
            t1.returns_tran_refno
        FROM tbl_wh_sales_returns t1
        LEFT JOIN tbl_item_data t2 
            ON t1.combination_id = t2.combination_id
        WHERE date(t1.created_date) BETWEEN %s AND %s
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
        st.write("❌ Error fetching sales data:", err)
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

def call_update_sales_returns1():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.callproc("UpdateSalesReturns1")
        conn.commit()
        cursor.close()
        conn.close()
        st.success("✅ Stored procedure 'UpdateSalesReturns1' executed successfully.")
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
        design_str = str(row["combination_id"])  # Ensure it's a string
        design_parts = design_str.split("-")  # Split by '-'
        
        # Only add split values (remove original row if it had "-")
        for part in design_parts:
            new_row = row.copy()
            new_row["combination_id"] = part.strip()  # Assign the new design number
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
    grouped_data = df_sales_2.groupby('returns_tran_refno')
    
    for returns_tran_refno, group in grouped_data:
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
            sr_bill_no = f"BGR{returns_tran_refno}"

            pdf.set_font("Arial", size=7)
            pdf.cell(40, 3, f"SR Bill No:         {sr_bill_no}", 0, 0)
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
            safe_refno = str(returns_tran_refno).replace("/", "_")
            pdf_output = f"sales_return_{safe_refno}.pdf"
            pdf.output(pdf_output)
            
            # Create download button
            with open(pdf_output, "rb") as f:
                pdf_bytes = f.read()
            
            b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
            download_link = f'<a href="data:application/pdf;base64,{b64_pdf}" download="{pdf_output}">Download PDF for returns_tran_refno:  {returns_tran_refno}</a>'
            
            pdf_downloads[returns_tran_refno] = download_link
            
            # Clean up temporary file
            os.remove(pdf_output)
            
        except Exception as e:
            st.error(f"Error generating PDF for returns_tran_refno {returns_tran_refno}: {str(e)}")
    
    return pdf_downloads

def display_sales_return_pdfs(df_sales_2):
    st.header("Generated Sales Return PDFs")
    
    if df_sales_2 is None or df_sales_2.empty:
        st.warning("No sales data available to generate PDFs.")
        return
    
    # Debug information
    st.write(f"Number of records in sales_returns: {len(df_sales_2)}")
    
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

# Add this inside your application code after your current page options
elif st.session_state.page == "upload":    
    options = ["RTV page", "RTO page", "SR page", "TO page"]

    if "sidebar_open" not in st.session_state:
        st.session_state.sidebar_open = True
    
    # Use the same key for both radios so selection persists
    if st.session_state.sidebar_open:
        selected_page = st.sidebar.radio("Select Page", options, key="upload_page")
    else:
        selected_page = st.radio("Select Page", options, key="upload_page")
    
    top_cols = st.columns([9, 1])
    with top_cols[1]:
        if st.button("Sidebar"):
            st.session_state.sidebar_open = not st.session_state.sidebar_open
            st.rerun()

    # Use selected_page variable instead of page variable
    if selected_page == "RTV page":
        col1, col2, col3 = st.columns([1.5, 8, 1.5])
        with col3:
            # GitHub raw file URL (make sure it's the raw version)
            template_url = "https://raw.githubusercontent.com/Shivarajkushals/centralized_returns/main/RTV%20Template.xlsx"

            st.markdown(
                f'<a href="{template_url}" download target="_blank">'
                f'<button style="background-color:#4CAF50;color:white;padding:8px 6px;border:none;border-radius:5px;cursor:pointer;">📥 Download Template</button>'
                f'</a>',
                unsafe_allow_html=True
            )
        with col2:
            # Streamlit UI
            st.title("RTV Page Upload")
            
            uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx"])
            
            if uploaded_file is not None:
                if uploaded_file.name.endswith(".csv"):
                    uploaded_df = pd.read_csv(uploaded_file)
                else:
                    try:
                        uploaded_df = pd.read_excel(uploaded_file, engine="openpyxl", dtype = str)
                    except ImportError:
                        st.error("Missing dependency: Please install 'openpyxl' using `pip install openpyxl`.")
                
                st.write("Uploaded file")
                st.dataframe(uploaded_df)
            
                # Standardizing column names (Fix for 'Stores' KeyError)
                uploaded_df.columns = uploaded_df.columns.str.strip().str.lower()
                
                uploaded_df = expand_design_numbers(uploaded_df)
                
                uploaded_df["qty"] = uploaded_df["combination_id"].astype(str).apply(calculate_qty)
                uploaded_df["barcode"] = uploaded_df["barcode"].astype(str)
                uploaded_df["barcode"] = uploaded_df["barcode"].astype(str).str.replace('"', '', regex=False)
                uploaded_df["bill no"] = uploaded_df["bill no"].astype(str).str.strip()
                uploaded_df["combination_id"] = uploaded_df["combination_id"].astype(str).str.strip()

                filter_tuples = list(
                    uploaded_df[[ "bill no", "combination_id", "barcode"]]
                    .dropna()
                    .drop_duplicates()
                    .itertuples(index=False, name=None)
                )

                placeholders = ', '.join(['( %s, %s, %s)'] * len(filter_tuples))
                flat_values = [item for tup in filter_tuples for item in tup]

                conn = mysql.connector.connect(**DB_CONFIG)
                cursor = conn.cursor(dictionary=True)

                query = f"""
                    SELECT t2.store_full_name as stores, t1.bill_number as `bill no`, t1.combination_id, t1.barcode
                    FROM tbl_sales t1
                    LEFT JOIN tbl_store_data t2 ON t1.outlets_id = t2.id
                    WHERE (t1.bill_number, t1.combination_id, t1.barcode) IN ({placeholders}) AND t1.bill_date >= CURDATE() - INTERVAL 181 DAY
                    GROUP BY t2.store_full_name, t1.bill_number, t1.combination_id, t1.barcode;
                """

                query1 = f"""
                    SELECT t2.store_full_name as stores, msr.GST_bill_number as `bill no`, t1.combination_id, t1.barcode
                    FROM tbl_sales t1 
                    INNER JOIN minimized_sales_register msr 
                    ON t1.bill_number = msr.bill_number AND t1.bill_date = msr.bill_date
                    LEFT JOIN tbl_store_data t2 ON t1.outlets_id = t2.id
                    WHERE (msr.GST_bill_number, t1.combination_id, t1.barcode) IN ({placeholders}) AND t1.bill_date >= CURDATE() - INTERVAL 181 DAY
                    GROUP BY t2.store_full_name, msr.GST_bill_number, t1.combination_id, t1.barcode;
                """
                
                cursor.execute(query1, flat_values)
                filtered_data = cursor.fetchall()
                df_filtered = pd.DataFrame(filtered_data)

                cursor.close()
                conn.close()

                expanded_df = df_filtered.copy()

                # Step 1: Merge the dataframes
                expanded_df = pd.merge(
                    expanded_df,
                    uploaded_df,
                    on=['stores', 'bill no', 'combination_id', 'barcode'],
                    how='left'
                )

                # Step 2: Identify unmatched rows based on the full key combination
                # Create a set of keys from expanded_df
                matched_keys = set(
                    tuple(x) for x in expanded_df[['stores', 'bill no', 'combination_id', 'barcode']].dropna().values
                )

                # Find rows in uploaded_df that did not match any in expanded_df
                missing_gst_bill_nos = uploaded_df[
                    ~uploaded_df[['stores', 'bill no', 'combination_id', 'barcode']]
                    .apply(tuple, axis=1)
                    .isin(matched_keys)
                ]

                # Step 3: Output unmatched and updated dataframes
                if missing_gst_bill_nos.empty:
                    st.info("All the uploaded records are valid")
                else:
                    st.write("Invalid data entries:")
                    st.dataframe(missing_gst_bill_nos)

                # Continue with the updated dataframe
                uploaded_df = expanded_df
                
                # Filter out inactive stores
                uploaded_df, inactive_df = filter_inactive_stores(uploaded_df)

                if not inactive_df.empty:
                    st.write("Inactive store data:")
                    st.dataframe(inactive_df)
            
                data = fetch_all_data()  # Fetch all required data in one go
                
                db_df = data["sales_returns_df"]  # tbl_wh_sales_returns data
                db_transfer_out = data["transfer_out_df"]  # tbl_wh_transfer_out data
                
                sr_dict = data["sr_numbers"]  # Max SR numbers per store
                to_dict = data["to_numbers"]  # Max TO numbers per store
                store_case_mapping = data["store_case_mapping"]  # Store name case mapping

                batch_no = data["next_batch_no"] # Max batch id for one time updation
            
                uploaded_df, duplicate_records = check_duplicates(uploaded_df, db_df)
                
                # Use the modified functions with store_case_mapping
                uploaded_df = assign_sr_numbers(uploaded_df, sr_dict)
                uploaded_df, max_to_dict = assign_to_numbers(uploaded_df, to_dict, store_case_mapping)

                if not duplicate_records.empty:
                    st.write("Duplicate records")
                    st.dataframe(duplicate_records)
                    
                    csv_duplicates = duplicate_records.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Duplicate Records", csv_duplicates, "duplicate_records.csv", "text/csv")
            
            
                if not uploaded_df.empty:
                    st.success("✅ Processing non-duplicate data...")
            
                    required_columns = [ "stores", "bill no", "design numbers", "qty", "date", "sr_no", "sr amount", "invoice no", "order no", "tender", "combination_id", "barcode"]
                    
                    for col in required_columns:
                        if col not in uploaded_df.columns:
                            st.error(f"❌ Missing required column: {col}")
                            st.stop()
                            
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    sr_df = uploaded_df[required_columns].copy()
                    sr_df.rename(columns={
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
                    sr_df["return_date"] = pd.to_datetime(sr_df["return_date"]).dt.strftime('%Y-%m-%d')
                    sr_df["created_by"] = "WH Team"
                    sr_df["modified_by"] = "WH Team"  
                    sr_df["tran_type"] = "Sales Returns"
                    sr_df["batch_no"] = batch_no 
                    sr_df["RTO"] = 0
            
                    to_df = uploaded_df[uploaded_df["to_no"] != ""].copy()
                    to_df = to_df[[ "stores", "to_no", "qty", "date", "combination_id", "bill no"]].copy()
                    to_df.rename(columns={"stores": "outlet_name_from",
                                          "to_no": "transfer_out_no",
                                          "date": "return_date",
                                          "bill no": "bill_no"}, inplace=True)
                    
                    # Adding additional constant columns
                    to_df["is_active"] = 1  
                    to_df["created_date"] = current_time  # Convert to string
                    to_df["modified_date"] = current_time 
                    to_df["created_by"] = "WH Team"
                    to_df["return_date"] = pd.to_datetime(to_df["return_date"]).dt.strftime('%Y-%m-%d')  
                    to_df["modified_by"] = "WH Team"  
                    to_df["branch_recived"] = "Banglore_WH" 
                    to_df["transfer_out_date"] = current_time
                    to_df["batch_no"] = batch_no
                    to_df["RTO"] = 0
            
                    rows_inserted = insert_data(sr_df, "tbl_wh_sales_returns")
                    rows_inserted = insert_data(to_df, "tbl_wh_transfer_out")
                    call_update_sales_returns1()
                    sr_to_max = update_store_max_sr_to(DB_CONFIG, max_to_dict)
            
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_sales_returns.")
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_transfer_out.")
                    
                    uploaded_df = uploaded_df.drop(['stores_lower', 'to_no'], axis=1)
                    csv_uploaded = uploaded_df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Updated CSV", csv_uploaded, "updated_data.csv", "text/csv")
            
                else:
                    st.warning("⚠️ No new data to process after removing duplicates.")

    elif selected_page == "RTO page":
        col1, col2, col3 = st.columns([1.5, 8, 1.5])
        with col3:
            # GitHub raw file URL (make sure it's the raw version)
            template_url = "https://raw.githubusercontent.com/Shivarajkushals/centralized_returns/main/RTO%20Template.xlsx"

            st.markdown(
                f'<a href="{template_url}" download target="_blank">'
                f'<button style="background-color:#4CAF50;color:white;padding:8px 6px;border:none;border-radius:5px;cursor:pointer;">📥 Download Template</button>'
                f'</a>',
                unsafe_allow_html=True
            )
        with col2:
            # Streamlit UI
            st.title("RTO Page Upload")
            
            uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx"], key="rto_uploader")
            
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

                # Find duplicate GST Bill Numbers
                duplicate_gst_bills = uploaded_df[uploaded_df.duplicated('bill no', keep=False)]

                if not duplicate_gst_bills.empty:
                    st.write("Duplicate GST Bill Numbers:")
                    st.dataframe(duplicate_gst_bills)

                # Remove duplicates based on 'bill no' and keep the first occurrence
                uploaded_df = uploaded_df.drop_duplicates(subset='bill no', keep='first')

                gst_bill_no = uploaded_df['bill no'].astype(str).tolist()

                placeholders = ', '.join(['%s'] * len(gst_bill_no))

                conn = mysql.connector.connect(**DB_CONFIG)
                cursor = conn.cursor(dictionary=True)

                # Format the placeholders and store list

                query = f"""
                    SELECT DISTINCT t2.combination_id, t1.bill_date , t1.bill_number, t1.GST_bill_number,
                                    t2.design_number, sum(sold_qty) as qty, t2.barcode
                    FROM minimized_sales_register t1
                    LEFT JOIN tbl_sales t2 ON t1.bill_number = t2.bill_number AND t1.bill_date = t2.bill_date
                    WHERE t1.GST_bill_number IN ({placeholders})
                    GROUP BY t2.combination_id, t1.bill_date, t1.bill_number, t1.GST_bill_number, t2.design_number, t2.barcode;
                """
                cursor.execute(query, tuple(gst_bill_no))
                filtered_data = cursor.fetchall()
                df_filtered = pd.DataFrame(filtered_data)

                cursor.close()
                conn.close()

                expanded_df = df_filtered.copy()
                
                # Step 1: Normalize keys for merge
                uploaded_df['bill no'] = uploaded_df['bill no'].astype(str).str.strip().str.upper()
                expanded_df['GST_bill_number'] = expanded_df['GST_bill_number'].astype(str).str.strip().str.upper()

                # Step 2: Prepare columns for matching
                # We'll merge on: bill no, combination_id, and design number
                expanded_df.rename(columns={
                    'GST_bill_number': 'bill no',
                    'design_number': 'design numbers'
                }, inplace=True)

                # Strip & match column names and types before merge
                uploaded_df['bill no'] = uploaded_df['bill no'].astype(str).str.strip().str.upper()

                expanded_df['bill no'] = expanded_df['bill no'].astype(str).str.strip().str.upper()
                expanded_df['combination_id'] = expanded_df['combination_id'].astype(str).str.strip()
                expanded_df['design numbers'] = expanded_df['design numbers'].astype(str).str.strip().str.upper()

                expanded_df = pd.merge(
                    expanded_df,
                    uploaded_df,
                    on='bill no',
                    how='left'
                )

                missing_gst_bill_nos = uploaded_df[~uploaded_df['bill no'].isin(expanded_df['bill no'])]

                if missing_gst_bill_nos.empty:
                    st.info ("All the recodrs are valied from uploaded file")
                else:
                    st.write("Invalid gst_bill_nos")
                    st.dataframe(missing_gst_bill_nos)

                uploaded_df = expanded_df

                uploaded_df, inactive_df = filter_inactive_stores(uploaded_df)

                if not inactive_df.empty:
                    st.write("Inactive store data:")
                    st.dataframe(inactive_df)
            
                data = fetch_all_data()  # Fetch all required data in one go
                
                db_df = data["sales_returns_df"]  # tbl_wh_sales_returns data
                db_transfer_out = data["transfer_out_df"]  # tbl_wh_transfer_out data
                
                sr_dict = data["sr_numbers"]  # Max SR numbers per store
                to_dict = data["to_numbers"]  # Max TO numbers per store
                store_case_mapping = data["store_case_mapping"]  # Store name case mapping

                batch_no = data["next_batch_no"] # Max batch id for one time updation
            
                uploaded_df, duplicate_records = check_duplicates(uploaded_df, db_df)
                
                # Use the modified functions with store_case_mapping
                uploaded_df = assign_sr_numbers(uploaded_df, sr_dict)
                uploaded_df, max_to_dict = assign_to_numbers(uploaded_df, to_dict, store_case_mapping)
            
                if not duplicate_records.empty:
                    st.write("Duplicate records")
                    st.dataframe(duplicate_records)
                    
                    csv_duplicates = duplicate_records.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Duplicate Records", csv_duplicates, "duplicate_records.csv", "text/csv")
            
            
                if not uploaded_df.empty:
                    st.success("✅ Processing non-duplicate data...")
            
                    required_columns = [ "stores", "bill no", "design numbers", "qty", "date", "sr_no", "sr amount", "invoice no", "order no", "tender", "combination_id", "barcode"]
                    
                    for col in required_columns:
                        if col not in uploaded_df.columns:
                            st.error(f"❌ Missing required column: {col}")
                            st.stop()
                            
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    sr_df = uploaded_df[required_columns].copy()
                    sr_df.rename(columns={
                        
                        "sr amount": "bill_amount",
                        "invoice no": "invoice_no",
                        "order no": "order_no",
                        "stores": "outlet_name", 
                        "bill no": "bill_no", 
                        "design numbers": "design_no", 
                        "qty": "Sold_qty", 
                        "date": "return_date",
                        "barcode": "barcode"
                    }, inplace=True)
                    
                    # Adding additional constant columns
                    sr_df["is_active"] = 1
                    sr_df["created_date"] = current_time  # Convert to string
                    sr_df["modified_date"] = current_time  
                    sr_df["return_date"] = pd.to_datetime(sr_df["return_date"]).dt.strftime('%Y-%m-%d')
                    sr_df["created_by"] = "WH Team"
                    sr_df["modified_by"] = "WH Team"  
                    sr_df["tran_type"] = "Sales Returns"
                    sr_df["batch_no"] = batch_no 
                    sr_df["RTO"] = 1
            
                    to_df = uploaded_df[uploaded_df["to_no"] != ""].copy()
                    to_df = to_df[[ "stores", "to_no", "qty", "date", "combination_id", "bill no"]].copy()
                    to_df.rename(columns={"stores": "outlet_name_from",
                                          "to_no": "transfer_out_no",
                                          "sales_return_id": "sr_id",
                                          
                                          "date": "return_date",
                                          "bill no": "bill_no"}, inplace=True)
                    
                    # Adding additional constant columns
                    to_df["is_active"] = 1  
                    to_df["created_date"] = current_time  # Convert to string
                    to_df["modified_date"] = current_time 
                    to_df["created_by"] = "WH Team"
                    to_df["return_date"] = pd.to_datetime(to_df["return_date"]).dt.strftime('%Y-%m-%d')  
                    to_df["modified_by"] = "WH Team"  
                    to_df["branch_recived"] = "Banglore_WH" 
                    to_df["transfer_out_date"] = current_time
                    to_df["batch_no"] = batch_no
                    to_df["RTO"] = 1
            
                    rows_inserted = insert_data(sr_df, "tbl_wh_sales_returns")
                    rows_inserted = insert_data(to_df, "tbl_wh_transfer_out")
                    call_update_sales_returns()
                    sr_to_max = update_store_max_sr_to(DB_CONFIG, max_to_dict)
            
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_sales_returns.")
                    st.success(f"✅ Inserted {rows_inserted} records into tbl_wh_transfer_out.")
                    
                    uploaded_df = uploaded_df.drop(['stores_lower', 'to_no'], axis=1)
                    csv_uploaded = uploaded_df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Updated CSV", csv_uploaded, "updated_data.csv", "text/csv")
            
                else:
                    st.warning("⚠️ No new data to process after removing duplicates.")
    
    elif selected_page == "SR page":
        st.subheader("🔍 Filter Sales returns Data")

        # UI filters
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", key="sr_start")
        with col2:
            end_date = st.date_input("End Date", key="sr_end")

        # Fetch distinct store names for dropdown
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT outlet_name FROM tbl_wh_sales_returns;")
        store_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        # Initialize session state for SR stores if not exists
        if "sr_select_all_checked" not in st.session_state:
            st.session_state.sr_select_all_checked = False

        # Add "Select All" checkbox
        select_all = st.checkbox("Select All Stores", key="sr_select_all", value=st.session_state.sr_select_all_checked)

        # Update session state when checkbox changes
        if select_all != st.session_state.sr_select_all_checked:
            st.session_state.sr_select_all_checked = select_all
            st.rerun()

        # Use store_names as default if "Select All" is checked
        default_stores = store_names if st.session_state.sr_select_all_checked else []
        selected_stores = st.multiselect("Select Store(s)", store_names, default=default_stores, key="sr_stores")

        if st.button("✅ Continue", key="sr_continue"):
            if not selected_stores:
                st.warning("Please select at least one store.")
            else:
                try:
                    conn = mysql.connector.connect(**DB_CONFIG)
                    cursor = conn.cursor(dictionary=True)

                    # Format the placeholders and store list
                    store_placeholders = ','.join(['%s'] * len(selected_stores))

                    query = f"""
                SELECT 
                    s.*,
                    t1.store_full_name,
                    t2.name AS "shipping state"
                FROM tbl_wh_sales_returns s
                LEFT JOIN tbl_store_data t1 
                    ON s.outlet_name = t1.store_full_name
                LEFT JOIN value_view_store t3 
                    ON t1.id = t3.store_data_id
                LEFT JOIN tbl_view_value t2 
                    ON t2.id = t3.view_value_id
                WHERE 
                    DATE(s.created_date) BETWEEN %s AND %s
                    AND t2.view_id = 3
                    AND t1.store_full_name IN ({store_placeholders})
                    """

                    params = [start_date, end_date] + selected_stores
                    cursor.execute(query, params)
                    filtered_data = cursor.fetchall()
                    df_filtered = pd.DataFrame(filtered_data)

                    query1 = f"""
                        SELECT 
                            s.bill_date,
                            s.tender,
                            s.outlet_name,
                            s.customer_name,
                            s.sr_no AS return_no,
                            s.return_date,
                            s.bill_no AS Bill_refno,
                            ROUND(SUM(s.bill_amount_1) + SUM(s.packing_charges), 2) AS total_amount,
                            SUM(s.sold_qty) AS qty,
                            SUM(s.item_gross) AS item_gross,
                            SUM(s.discount_amount) AS discount_amount,
                            s.sales_tran_refno,
                            s.returns_tran_refno,
                            ROUND(SUM(s.bill_amount_1), 2) AS item_charges,
                            ROUND(SUM(s.packing_charges), 2) AS packing_charges,
                            s.customer_state,
                            s.mobile_number,
                            s.gst_billno,
                            SUM(s.gstamt) AS gst_amt,
                            SUM(s.cgst_amt) AS cgst_amt,
                            SUM(s.sgst_amt_ugst_amt) AS sgst_amt_ugst_amt,
                            s.hsn_sac_code,
                            t2.name AS "shipping state"
                        FROM tbl_wh_sales_returns s
                        LEFT JOIN tbl_store_data t1 
                            ON s.outlet_name = t1.store_full_name
                        LEFT JOIN value_view_store t3 
                            ON t1.id = t3.store_data_id
                        LEFT JOIN tbl_view_value t2 
                            ON t2.id = t3.view_value_id
                        WHERE 
                            DATE(s.created_date) BETWEEN %s AND %s
                            AND t2.view_id = 3
                            AND t1.store_full_name IN ({store_placeholders})
                        GROUP BY 
                            s.outlet_name, 
                            s.bill_no,
                            t2.name
                    """

                    params1 = [start_date, end_date] + selected_stores
                    cursor.execute(query1, params1)
                    filtered_data1 = cursor.fetchall()
                    df_filtered1 = pd.DataFrame(filtered_data1)

                    cursor.close()
                    conn.close()

                    if df_filtered.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Item_wise data from `tbl_wh_sales_returns`:")
                        st.dataframe(df_filtered.reset_index(drop=True))

                    if df_filtered1.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Bill_wise data from `tbl_wh_sales_returns`:")
                        st.dataframe(df_filtered1.reset_index(drop=True))
                except Exception as e:
                    st.error(f"❌ Error querying data: {e}")
                    
            _, df_sales_2 = fetch_sales_data(DB_CONFIG, start_date, end_date, selected_stores)
            to_display = pd.DataFrame(df_sales_2)

            st.write("SR PDF output:")
            display_df = to_display
            st.dataframe(display_df)

            if df_sales_2 is not None and not df_sales_2.empty:
                display_sales_return_pdfs(df_sales_2)
    
    elif selected_page == "TO page":
        st.subheader("🔍 Filter Transfer Out Data")

        # UI filters
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", key="to_start")
        with col2:
            end_date = st.date_input("End Date", key="to_end")

        # Fetch distinct store names for dropdown
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT outlet_name_from FROM tbl_wh_transfer_out;")
        store_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        # Initialize session state for TO stores if not exists
        if "to_select_all_checked" not in st.session_state:
            st.session_state.to_select_all_checked = False

        # Add "Select All Stores" checkbox
        select_all = st.checkbox("Select All Stores", key="to_select_all", value=st.session_state.to_select_all_checked)

        # Update session state when checkbox changes
        if select_all != st.session_state.to_select_all_checked:
            st.session_state.to_select_all_checked = select_all
            st.rerun()

        # Use store_names as default if "Select All" is checked
        default_stores = store_names if st.session_state.to_select_all_checked else []
        selected_stores = st.multiselect("Select Store(s)", store_names, default=default_stores, key="to_stores")

        if st.button("✅ Continue", key="to_continue"):
            if not selected_stores:
                st.warning("Please select at least one store.")
            else:
                try:
                    conn = mysql.connector.connect(**DB_CONFIG)
                    cursor = conn.cursor(dictionary=True)

                    # Format the placeholders and store list
                    store_placeholders = ','.join(['%s'] * len(selected_stores))

                    query = f"""
                        SELECT 
                            t.*,
                            t2.name AS "shipping state"
                        FROM tbl_wh_transfer_out t
                        LEFT JOIN tbl_store_data t1 
                            ON t.outlet_name_from = t1.store_full_name
                        LEFT JOIN value_view_store t3 
                            ON t1.id = t3.store_data_id
                        LEFT JOIN tbl_view_value t2 
                            ON t2.id = t3.view_value_id
                        WHERE 
                            DATE(t.created_date) BETWEEN %s AND %s
                            AND t2.view_id = 3
                            AND t1.store_full_name IN ({store_placeholders})
                    """

                    params = [start_date, end_date] + selected_stores
                    cursor.execute(query, params)
                    filtered_data = cursor.fetchall()
                    df_filtered = pd.DataFrame(filtered_data)

                    query1 = f"""
                        SELECT 
                            t.branch_recived AS `branch_name_(received_to)`,
                            t.outlet_name_from AS `outlet_name_(sent_from)`,
                            t.transaction_refno,
                            t.transfer_out_date,
                            ROUND(SUM(t.qty), 2) AS Tout_qty,
                            ROUND(SUM(t.item_cost), 2) AS pur_price,
                            ROUND(SUM(t.mrp), 2) AS MRP,
                            t2.name AS "shipping state"
                        FROM tbl_wh_transfer_out t
                        LEFT JOIN tbl_store_data t1 
                            ON t.outlet_name_from = t1.store_full_name
                        LEFT JOIN value_view_store t3 
                            ON t1.id = t3.store_data_id
                        LEFT JOIN tbl_view_value t2 
                            ON t2.id = t3.view_value_id
                        WHERE 
                            DATE(t.created_date) BETWEEN %s AND %s
                            AND t2.view_id = 3
                            AND t1.store_full_name IN ({store_placeholders})
                        GROUP BY 
                            t.branch_recived,
                            t.outlet_name_from,
                            t.transaction_refno,
                            t.transfer_out_date,
                            t2.name
                    """

                    params1 = [start_date, end_date] + selected_stores
                    cursor.execute(query1, params1)
                    filtered_data1 = cursor.fetchall()
                    df_filtered1 = pd.DataFrame(filtered_data1)

                    cursor.close()
                    conn.close()

                    if df_filtered.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Filtered data from `tbl_wh_transfer_out`:")
                        st.dataframe(df_filtered.reset_index(drop=True))

                    if df_filtered1.empty:
                        st.info("No data found for the selected filters.")
                    else:
                        st.write("📦 Filtered data from `tbl_wh_transfer_out`:")
                        st.dataframe(df_filtered1.reset_index(drop=True))
                    
                except Exception as e:
                    st.error(f"❌ Error querying data: {e}")
            
            df_sales_1, _ = fetch_sales_data(DB_CONFIG, start_date, end_date, selected_stores)
            to_display = pd.DataFrame(df_sales_1)
            
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
