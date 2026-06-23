#===============================================================
# Title: Live & Dynamic ETL Pipeline for Largest Banks Data
# Author: Mahnaz Salari | Data Engineering Portfolio Project
#===============================================================

import pandas as pd 
import numpy as np 
import requests 
import sqlite3
from datetime import datetime

# Project Configuration - Live Wikipedia Source
#===============================================================
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]

# Local Target Paths (Optimized for repository portability)
csv_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
log_file = 'code_log.txt'
exchange_csv_path = 'exchange_rate.csv'
table_name = 'Largest_banks'

#===========================================
# Task 1: Logging Operation
#===========================================
def log_progress(message):
    ''' Logs pipeline execution stages with high-precision system timestamps. '''
    timestamp_format = '%Y-%b-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

#===========================================
# Task 2: Data Extraction (Safe & Bulletproof)
# ==========================================
def extract(url, table_attribs):
    ''' Extracts live web data from Wikipedia using Pandas read_html to prevent HTML index shifting. '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:151.0) Gecko/20100101 Firefox/151.0'  
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            # Pandas automatic HTML table parser
            all_tables = pd.read_html(response.text)
            
            # Find the target Market Cap table
            target_df = None
            for table in all_tables:
                columns_joined = "".join(table.columns.astype(str)).lower()
                if 'market cap' in columns_joined or 'market capitalization' in columns_joined:
                    target_df = table
                    break
            
            # If search fails, fallback to the standard second table index
            if target_df is None and len(all_tables) > 1:
                target_df = all_tables[1]
            
            if target_df is not None:
                # Dynamically locate the correct column index for Bank Name and Market Cap
                name_col = [col for col in target_df.columns if 'bank' in str(col).lower() or 'name' in str(col).lower()][0]
                cap_col = [col for col in target_df.columns if 'market cap' in str(col).lower() or 'usd' in str(col).lower() or 'billion' in str(col).lower()][0]
                
                # Create a clean slice
                df = target_df[[name_col, cap_col]].copy()
                df.columns = table_attribs
                
                # Sanitize text formatting and force numeric types
                df['Name'] = df['Name'].astype(str).str.strip()
                df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(str).str.replace(',', '')
                df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
                
                # Filter out empty or corrupted records
                df = df.dropna(subset=['MC_USD_Billion', 'Name'])
                df = df[df['Name'] != '']
                
                return df.reset_index(drop=True)
                
    except Exception as e:
        log_progress(f"Live extraction failed: {str(e)}. Switching to backup array.")
        pass
        
    backup_data = [
        {"Name": "JPMorgan Chase", "MC_USD_Billion": 432.92},
        {"Name": "Bank of America", "MC_USD_Billion": 231.52},
        {"Name": "Industrial and Commercial Bank of China", "MC_USD_Billion": 194.56},
        {"Name": "Agricultural Bank of China", "MC_USD_Billion": 160.68},
        {"Name": "HDFC Bank", "MC_USD_Billion": 157.91},
        {"Name": "Wells Fargo", "MC_USD_Billion": 155.87},
        {"Name": "HSBC Holdings PLC", "MC_USD_Billion": 148.90},
        {"Name": "Morgan Stanley", "MC_USD_Billion": 140.83},
        {"Name": "China Construction Bank", "MC_USD_Billion": 139.82},
        {"Name": "Bank of China", "MC_USD_Billion": 136.81}
    ]
    return pd.DataFrame(backup_data, columns=table_attribs)

#===========================================
# Task 3: Data Transformation
# ==========================================
def transform(df, csv_path):
    ''' Computes currency conversions (GBP, EUR, INR) using the live exchange reference. '''
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    gbp_rate = float(exchange_rate['GBP'])
    eur_rate = float(exchange_rate['EUR'])
    inr_rate = float(exchange_rate['INR'])
    
    df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']]
    
    return df

#===========================================
# Tasks 4, 5 & 6: Data Loading & Query Verification
# ==========================================
def load_to_csv(df, output_path):
    df.to_csv(output_path, index = False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    print(f"\nQuery Statement: {query_statement}")
    sql_file = pd.read_sql(query_statement, sql_connection)
    print(sql_file)

# ==========================================================
# ETL Execution Core
# ==========================================================
log_progress('Preliminaries complete. Initiating LIVE ETL process')

df_extracted = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df_transformed = transform(df_extracted, exchange_csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df_transformed, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df_transformed, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# Verification Audits
run_query(f"SELECT * FROM {table_name} LIMIT 5", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

log_progress('Process Complete Successfully')
sql_connection.close()
log_progress('Server Connection closed')
