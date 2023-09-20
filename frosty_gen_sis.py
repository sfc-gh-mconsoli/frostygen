import streamlit as st
import pandas as pd
import random
import uuid
import snowflake.connector
import numpy as np
import base64
from datetime import datetime, timedelta
from snowflake.snowpark import Session, FileOperation
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark.context import get_active_session


# FrostyGen - Random Data Generator - Migration to SiS
# Author: Matteo Consoli 
# Artifact: frosty_gen_sis.py 
# Version: v.1.0 
# Date: 18-09-2023

### --------------------------- ###
### Header & Config             ###  
### --------------------------- ###

# Set page title, icon, description
st.set_page_config(
    page_title="FrostyGen - Random Data Generator",
    page_icon="logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("FrostyGen - Random Data Generator ")
st.subheader("#Generate #Iced? #Data")

# Variable to avoid error on df.write before generating data.
df_exists = False

### --------------------------- ###
### Snowflake Connection        ###  
### --------------------------- ###
# Compared to standalone streamlit, I can get directly from Snowsight session details. I'll drop in the rest of the code the input forms for connecting to Snowflake instance.
def get_snowflake_connection():
        return get_active_session()
st.session_state.snowflake_connection = get_active_session()


### --------------------------- ###
### Main Page - Formats Config  ###  
### --------------------------- ###
cols=st.columns(4)
with cols[0]:
    num_records = st.number_input("# of Records to Generate", min_value=1, max_value=1000000, value=10, step=1)
with cols[1]:
    num_fields = st.number_input("# of Fields to Create", min_value=2, max_value=20, step=1)
with cols[2]:
    file_separator = st.selectbox("Field Separator", [",",";",":","|","#"],key=f"separator")
with cols[3]:
    include_header = st.selectbox("Include Header?", ["Yes", "No"])

st.markdown("""----""")

### ------------------------------- ###
### Main Page - Data Fields Config  ###  
### ------------------------------- ###
st.header("Define Data Fields")

user_string_values = {}
table_string_values = {}

# Crazy part. Once number of fields is defined, the for loop is cycling and setting configs for all of them.
field_config = []

for i in range(num_fields):
    cols_field=st.columns(2)
    with cols_field[0]:
        field_name=st.text_input(f"Field {i+1}: Name", f"FIELD_NAME_{i}", key=f"field_name{i}")
    with cols_field[1]:
        field_type=st.selectbox(f"Field {i+1}: Type", ["Integer", "Text", "DateTime", "Double", "UUID", "DatabaseColumn"], key=f"field_type_{i}")   
        #--------------
        #DateTime Config
        #--------------
        if field_type == "DateTime":
            selected_date = st.date_input("Select a date", datetime.today(),key=f"field_date{i}")
            num_days = st.number_input("Range of days for random date", min_value=1, value=7, key=f"field_date_number_input{i}" )
        else:
            selected_date = None
            num_days = 0
        #--------------
        #Integer Config
        #--------------
        if field_type == "Integer":
            min_int_value = st.number_input("Min Random Value", min_value=0, value=1, key=f"min_value_integer_{i}" )
            max_int_value = st.number_input("Max Random Value", min_value=1, value=100, key=f"max_value_integer_{i}" )
        else:
            min_int_value = 0
            max_int_value = 0
        #--------------
        #Database column lookup
        #--------------
        if field_type == "DatabaseColumn":
            selected_column = None
            if (hasattr(st.session_state, 'snowflake_connection')):
                session = get_active_session()
                cursor = session.sql("SHOW DATABASES").collect()
                databases = [row[1] for row in cursor]
                # Select Database
                selected_database = st.selectbox("Select Database", databases, key=f"selected_db_name_{i}" )
                if selected_database:
                # Fetch schemas in the selected database
                    cursor = session.sql(f"SHOW SCHEMAS IN DATABASE {selected_database}").collect()
                    schemas = [row[1] for row in cursor]
                    # Select Schema
                    selected_schema = st.selectbox("Select Schema", schemas, index=0, key=f"selected_schema_name_{i}" )
                    if selected_schema is not None :
                    # Fetch tables in the selected schema
                        cursor = session.sql(f"SHOW TABLES IN SCHEMA {selected_database}.{selected_schema}").collect()
                        tables = [row[1] for row in cursor]
                        # Select Table
                        selected_table = st.selectbox("Select Table", tables, key=f"selected_table_name_{i}")
                        if selected_table is not None:
                            # Fetch columns in the selected table
                            cursor = session.sql(f"DESCRIBE TABLE {selected_database}.{selected_schema}.{selected_table}").collect()
                            columns = [row[0] for row in cursor]
                            # Select Column
                            selected_column = st.selectbox("Select Column", columns, key=f"selected_column_name_{i}")
                            select_limit_distinct = st.number_input("# of distinct records to extract", min_value=1, value=10, max_value=1000, key=f"selected_limit_name_{i}")
                # Generate and execute the query
                if selected_column:
                    query = f"SELECT DISTINCT {selected_column} FROM {selected_database}.{selected_schema}.{selected_table} LIMIT {select_limit_distinct}"
                    cursor = session.sql(query).collect()
                    # Fetch distinct values from the column
                    distinct_values = [row[0] for row in cursor]
                    result = cursor
                    result_text = "\n".join(str(row) for row in distinct_values)
                    table_field_values = st.text_area("Extracted Values for Field", value=result_text, key=f"table_field_values_{i}")
                    table_string_values[field_name] = table_field_values.split('\n')

            else:
                st.warning("Connect to Snowflake first.")
        #--------------
        #Text data type Configuration
        #--------------
        if field_type == "Text":
            text_input_option = st.radio("Text Input Option", ["Write Values (One per line)", "Auto-generate based on Length, Prefix, and Suffix"], key=f"text_input_option_{i}")
            if text_input_option == "Write Values (One per line)":
                field_values = st.text_area("Values for Field", key=f"field_values_{i}")
                user_string_values[field_name] = field_values.split('\n')
            else:
                string_length = st.number_input("String Length", min_value=1, step=1, key=f"string_length_{i}")
                prefix = st.text_input("Prefix", key=f"prefix_{i}")
                suffix = st.text_input("Suffix", key=f"suffix_{i}")
                category = st.selectbox("Category", ["Letters", "Digits", "Alphanumeric"], key=f"category_{i}")
                if category == "Letters":
                    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                elif category == "Digits":
                    chars = "0123456789"
                else:
                    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                # Generate 100 random strings and store them in a list
                random_strings = [f"{prefix}{''.join(random.choice(chars) for _ in range(string_length))}{suffix}" for _ in range(100)]
                user_string_values[field_name] = random_strings
    
    st.markdown("""----""")
    #--------------
    #Other data types don't require further configuration, they will be directly randomly generated. Field_config contains attributes configured above.
    #--------------
    field_config.append({"name": field_name, "type": field_type, "selected_date": selected_date,"num_days": num_days, "min_int_value":min_int_value, "max_int_value": max_int_value})

### ---------------------------- ###
### Sidebar - Configurations     ### 
### ---------------------------- ###
image_name = 'logo.png'
mime_type = image_name.split('.')[-1:][0].lower()        
with open(image_name, "rb") as f:
    content_bytes = f.read()
content_b64encoded = base64.b64encode(content_bytes).decode()
image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
st.sidebar.image(image_string)

### --------------------------- ###
### Sidebar - ExportOptions     ###  
### --------------------------- ###
st.sidebar.header("Export Your Dataset")
export_option = st.sidebar.selectbox("Export Options", ["Export to Snowflake Stage", "Export to Snowflake Table", "Save to File"])

if export_option == "Export to Snowflake Stage":
    if (hasattr(st.session_state, 'snowflake_connection')):
        snowflake_stage = st.sidebar.text_input("Snowflake Stage")
        # Snowflake export details
        file_prefix = st.sidebar.text_input("File Prefix", "data.csv")
        file_suffix = "" 
    else:
        st.sidebar.warning("You are not connected to Snowflake yet.")

elif export_option == "Export to Snowflake Table": 
    if (hasattr(st.session_state, 'snowflake_connection')):
        database_name = st.sidebar.text_input("Database Name")
        schema_name = st.sidebar.text_input("Schema Name")
        table_name = st.sidebar.text_input("Table Name")
        table_strategy = st.sidebar.selectbox("Table Strategy", ["CREATE IF NOT EXISTS", "CREATE OR REPLACE"],index=0)
        #st.sidebar.write("NOTE: Table will be created if it doesn't exist.")
    else:
        st.sidebar.warning("You are not connected to Snowflake yet.")  
else:
    # File export details
    file_prefix = st.sidebar.text_input("File Prefix", "data.csv")
    file_suffix = "" 
    st.sidebar.warning(f"This feature is not available yet on SiS FrostyGen. \n\n Only Preview Available")
    #Currently not implemented yet, possibility to split in multiple files the output 
    #max_records_per_file = st.sidebar.number_input("Max Records per File", min_value=1)

### --------------------------- ###
### Sidebar - Export Engine     ###  
### --------------------------- ###
# Generate data and create CSV or export to Snowflake
if st.sidebar.button("Export Data"):
    if num_records <= 0:
        st.sidebar.error("Number of records must be greater than 0.")
    elif num_fields <= 0:
        st.sidebar.error("Number of fields must be greater than 0.")
    else:
        data = []
# Generate random values at this point.
        for _ in range(num_records): 
            record = {}
            for field in field_config:
                field_name = field["name"]
                field_type = field["type"]
                
                if (field_type == "Text"):
                    record[field_name] = random.choice(user_string_values[field_name])
                elif field_type == "DatabaseColumn":
                    record[field_name] = random.choice(table_string_values[field_name])
                elif field_type == "Integer":
                    field_min_int_value = field["min_int_value"]
                    field_max_int_value = field["max_int_value"]
                    record[field_name] = random.randint(field_min_int_value, field_max_int_value)
                elif field_type == "DateTime":
                    field_selected_date = field["selected_date"]
                    field_num_days = field["num_days"]
                    date_range = pd.date_range(start=field_selected_date - pd.DateOffset(days=field_num_days),
                              end=field_selected_date + pd.DateOffset(days=field_num_days))
                    record[field_name] = np.random.choice(date_range)
                elif field_type == "Double":
                    record[field_name] = round(random.uniform(0, 1), 2)
                elif field_type == "UUID":
                    record[field_name] = str(uuid.uuid4())
            data.append(record)
# Finally, the dataframe is ready! 
        df = pd.DataFrame(data)
        df_exists = True

### --------------------------- ###
### Sidebar - Export Logic      ###  
### --------------------------- ###
        if export_option == "Save to File":
            # Currently not implemented yet -> Split data into multiple CSV files if necessary
            #st.sidebar.success(f"Data generated. Click the button to download {file_name}")
            #Feature not available yet
            currently_not_implemented=0
        elif export_option == "Export to Snowflake Stage":
            try:
                file_name = f"{file_prefix}{file_suffix}"
                file_csv = df.to_csv(file_name, index=False, header=include_header, sep=file_separator)
                session = get_snowflake_connection()
                # Create internal stage if it does not exists
                session.sql(f"create stage if not exists {snowflake_stage} ").collect()
                snowpark_df = session.create_dataframe(df)
                
                snowpark_df.write.copy_into_location(
                    f"{snowflake_stage}/{file_name}",
                    overwrite=True,
                    single=True,
                )
                st.sidebar.success(f"Data exported to Snowflake stage {snowflake_stage}")
            except Exception as e:
                st.sidebar.error(f"Error exporting data to Snowflake: {str(e)}")
        elif export_option == "Export to Snowflake Table":
            session = get_snowflake_connection()
            table_name_full = f"{database_name}.{schema_name}.{table_name}";
            session.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}").collect()
            session.sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}").collect()
            if table_strategy == "CREATE OR REPLACE":
                session.sql(f"CREATE OR REPLACE TABLE "+table_name_full+" ( " +' VARCHAR(100), '.join(df.columns)+" VARCHAR(100))").collect()
            else:
                session.sql(f"CREATE TABLE IF NOT EXISTS "+table_name_full+" ( " +' VARCHAR(100), '.join(df.columns)+" VARCHAR(100))").collect()
            session.sql(f"USE DATABASE {database_name};") 
            try: 
                session.write_pandas(df,table_name, schema=schema_name, database=database_name, auto_create_table=True)
                st.sidebar.success(f"Table '{table_name}' created successfully in Snowflake!")
            except Exception as e:
                st.error(f"Error while writing on Database: {e}")

### --------------------------- ###
### Footer and Preview          ###  
### --------------------------- ###
st.sidebar.text("Author: Matteo Consoli")

# Display the generated data in a table preview
if df_exists:
        st.header("Generated Data Sample")
        st.table(df.head(10))
else:
    st.warning("Select export option and click the export button to see a preview.")
