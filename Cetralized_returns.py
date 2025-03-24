import streamlit as st
import pandas as pd
import mysql.connector
import requests
import os
import git
import json

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

        # Fetch most recent records based on created_date
        cursor.execute("""
            SELECT * FROM tbl_wh_sales_returns 
            WHERE created_date = (SELECT MAX(created_date) FROM tbl_wh_sales_returns);
        """)
        recent_sales_returns = cursor.fetchall()

        cursor.execute("""
            SELECT * FROM tbl_wh_transfer_out 
            WHERE created_date = (SELECT MAX(created_date) FROM tbl_wh_transfer_out);
        """)
        recent_transfer_out = cursor.fetchall()

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
            "recent_sales_returns_df": pd.DataFrame(recent_sales_returns),
            "recent_transfer_out_df": pd.DataFrame(recent_transfer_out),
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
            "recent_sales_returns_df": pd.DataFrame(),
            "recent_transfer_out_df": pd.DataFrame(),
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
        existing_lower = {row[0].lower(): row[0] for row in cursor.fetchall()}
        
        # Prepare update statements
        for store in set(max_sr_dict) | set(max_to_dict):
            new_sr = max_sr_dict.get(store)
            new_to = max_to_dict.get(store)
            
            # Check if store exists (case-insensitive)
            store_exists = False
            existing_store = None
            
            # Check if store exists with any case
            for db_store in existing_data:
                if db_store.lower() == store.lower():
                    store_exists = True
                    existing_store = db_store
                    break
            
            if store_exists:
                update_query = """
                UPDATE tbl_wh_store_config 
                SET max_sr = %s, max_to = %s
                WHERE store_name = %s
                """
                cursor.execute(update_query, (new_sr, new_to, existing_store))
            else:
                # If store doesn't exist, insert it
                insert_query = """
                INSERT INTO tbl_wh_store_config (store_name, max_sr, max_to)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (store, new_sr, new_to))
        
        # Commit and close
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Store max SR and TO numbers updated successfully!")
        st.success("✅ Store max SR and TO numbers updated successfully!")
        
    except mysql.connector.Error as err:
        print("Error:", err)
        st.error(f"❌ Error updating store config: {err}")

# Function to calculate Qty based on "-" in Design Numbers
def calculate_qty(design_number):
    return str(design_number).count("-") + 1 if pd.notna(design_number) else 1

# Function to insert data into tbl_wh_sales_returns
def insert_sales_returns(sr_df):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        columns = ", ".join(sr_df.columns)
        placeholders = ", ".join(["%s"] * len(sr_df.columns))
        
        query = f"INSERT INTO tbl_wh_sales_returns ({columns}) VALUES ({placeholders})"
        
        values = [tuple(row) for row in sr_df.to_numpy()]
        
        cursor.executemany(query, values)
        conn.commit()
        
        rows_affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        return rows_affected
        
    except Exception as e:
        st.error(f"❌ Error inserting data into tbl_wh_sales_returns: {e}")
        return 0

# Function to insert data into tbl_wh_transfer_out
def insert_transfer_out(to_df):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        columns = ", ".join([f"{col}" for col in to_df.columns])
        placeholders = ", ".join(["%s"] * len(to_df.columns))
        
        query = f"INSERT INTO tbl_wh_transfer_out ({columns}) VALUES ({placeholders})"
        
        values = [tuple(row) for row in to_df.to_numpy()]
        
        cursor.executemany(query, values)
        conn.commit()
        
        rows_affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        return rows_affected
        
    except Exception as e:
        st.error(f"❌ Error inserting data into tbl_wh_transfer_out: {e}")
        return 0

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
                uploaded_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
                
                st.write("Uploaded file")
                st.dataframe(uploaded_df)
            
                # Standardizing column names (Fix for 'Stores' KeyError)
                uploaded_df.columns = uploaded_df.columns.str.strip().str.lower()
                
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
            
                    required_columns = ["sales_return_id", "stores", "bill no", "design numbers", "qty", "date", "sr_no", "sr amount", "invoice no", "order no", "tender"]
                    
                    for col in required_columns:
                        if col not in uploaded_df.columns:
                            st.error(f"❌ Missing required column: {col}")
                            st.stop()
            
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
                    sr_df["created_date"] = pd.Timestamp.now()  
                    sr_df["modified_date"] = pd.Timestamp.now()  
                    sr_df["created_by"] = "WH Team"  
                    sr_df["modified_by"] = "WH Team"  
                    sr_df["tran_type"] = "Sales Returns" 
            
                    to_df = uploaded_df[["transfer_out_id", "stores", "to_no", "qty","sales_return_id",]].copy()
                    to_df.rename(columns={"stores": "outlet_name_from",
                                          "to_no": "transfer_out_no",
                                          "sales_return_id": "sr_id",
                                          "transfer_out_id": "id"}, inplace=True)
                    
                    # Adding additional constant columns
                    to_df["is_active"] = 1  
                    to_df["created_date"] = pd.Timestamp.now()  
                    to_df["modified_date"] = pd.Timestamp.now()  
                    to_df["created_by"] = "WH Team"  
                    to_df["modified_by"] = "WH Team"  
                    to_df["branch_recived"] = "Banglore_WH" 
                    to_df["transfer_out_date"] = pd.Timestamp.now()
            
                    sr_inserted = insert_sales_returns(sr_df)
                    to_inserted = insert_transfer_out(to_df)
                    sr_to_max = update_store_max_sr_to(DB_CONFIG, max_sr_dict, max_to_dict)
            
                    st.success(f"✅ Inserted {sr_inserted} records into tbl_wh_sales_returns.")
                    st.success(f"✅ Inserted {to_inserted} records into tbl_wh_transfer_out.")
            
                    csv_uploaded = uploaded_df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Updated CSV", csv_uploaded, "updated_data.csv", "text/csv")
            
                else:
                    st.warning("⚠️ No new data to process after removing duplicates.")
        pass
    
    elif page == "SR page":
        data = fetch_all_data()  # Fetch all required data in one go
        
        st.write("Data from database table tbl_wh_sales_returns:")
        db_df = data["sales_returns_df"]
        st.dataframe(db_df.reset_index(drop=True))
                
        st.write("Recently uploaded data:")
        recent_df = data["recent_sales_returns_df"]
        st.dataframe(recent_df)
        pass
    
    elif page == "TO page":
        data = fetch_all_data()  # Fetch all required data in one go
        
        st.write("Data from database table tbl_wh_transfer_out:")
        db_df = data["transfer_out_df"]
        st.dataframe(db_df.reset_index(drop=True))
                
        st.write("Recently uploaded data:")
        recent_df = data["recent_transfer_out_df"]
        st.dataframe(recent_df)
        pass
    
            
            # Add a button to create the table if it doesn't exist
# =============================================================================
#             if st.button("Initialize Store Configuration Table"):
#                 try:
#                     conn = mysql.connector.connect(**DB_CONFIG)
#                     cursor = conn.cursor()
#                     
#                     # Check if table exists
#                     cursor.execute("SHOW TABLES LIKE 'tbl_wh_store_config'")
#                     table_exists = cursor.fetchone()
#                     
#                     if not table_exists:
#                         # Create the table if it doesn't exist
#                         create_table_query = """
#                         CREATE TABLE IF NOT EXISTS tbl_wh_store_config (
#                             id INT AUTO_INCREMENT PRIMARY KEY,
#                             store_name VARCHAR(255) NOT NULL,
#                             max_sr VARCHAR(20),
#                             max_to VARCHAR(20),
#                             is_active TINYINT(1) DEFAULT 1,
#                             created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                             modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#                         )
#                         """
#                         cursor.execute(create_table_query)
#                         st.success("✅ Store configuration table created successfully!")
#                     else:
#                         # If table exists but is_active column is missing, add it
#                         cursor.execute("SHOW COLUMNS FROM tbl_wh_store_config LIKE 'is_active'")
#                         column_exists = cursor.fetchone()
#                         
#                         if not column_exists:
#                             alter_table_query = """
#                             ALTER TABLE tbl_wh_store_config
#                             ADD COLUMN is_active TINYINT(1) DEFAULT 1
#                             """
#                             cursor.execute(alter_table_query)
#                             st.success("✅ Added is_active column to store configuration table!")
#                     
#                     conn.commit()
#                     cursor.close()
#                     conn.close()
#                     
#                     # Rerun to refresh the page
#                     st.rerun()
#                     
#                 except Exception as e:
#                     st.error(f"❌ Error initializing store configuration table: {e}")
# =============================================================================
