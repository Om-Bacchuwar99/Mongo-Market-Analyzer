import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from datetime import datetime
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "FinancialDataDB"
COLLECTION_NAME = "StockPrices"

TICKER = "MSFT"      
START_DATE = "2020-01-01"
END_DATE = "2024-01-01"

# 1. Connect to MongoDB

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB server.")
except Exception as e:
    print(f"❌ Error connecting to MongoDB. Is the server running? Error: {e}")
    exit()

# 2. Check and Create Time Series Collection

try:
    db.create_collection(
        COLLECTION_NAME,
        timeseries={"timeField": "Date", "metaField": "Ticker", "granularity": "hours"}
    )
    print(f"✅ Created Time Series Collection: {COLLECTION_NAME}")
except Exception as e:
    if "already exists" not in str(e):
        print(f"❌ Error creating collection: {e}")
    else:
        print("ℹ️ Collection already exists, proceeding...")

collection = db[COLLECTION_NAME]

# 3. Download Data using yfinance

print(f"⬇️ Downloading historical data for {TICKER}...")
df = yf.download(TICKER, start=START_DATE, end=END_DATE)
df = df.reset_index() 

df.columns = ['_'.join(map(str, col)).strip() if isinstance(col, tuple) else col for col in df.columns]

# 4. Standardize and Prepare Data

# i) Flatten and Convert all column names to lowercase for robust matching
df.columns = [str(col).lower().replace(' ', '_') for col in df.columns]

# ii) Identify and Standardize the date column name to 'date'
date_column_name = None
for col in df.columns:
    if 'date' in col or 'index' in col:
        date_column_name = col
        break
if not date_column_name:
    print(f"❌ FATAL ERROR: Could not find any date or index column. Columns: {df.columns.tolist()}")
    exit()
if date_column_name != 'date':
    df.rename(columns={date_column_name: 'date'}, inplace=True)


# iii) Ensure the 'close' price is available and standardized.
close_column = None
if 'close_msft' in df.columns.tolist():
    df.rename(columns={'close_msft': 'close'}, inplace=True)
    close_column = 'close'
elif 'close' in df.columns.tolist():
    close_column = 'close'
else:
    print(f"❌ FATAL ERROR: Column name issue persists. Columns: {df.columns.tolist()}")
    exit()

# iv) Filter out any rows with NaN values in the 'close' price
df.dropna(subset=[close_column], inplace=True)

# v) Convert 'date' column to datetime objects
df['date'] = pd.to_datetime(df['date']) 

# vi) Rename all necessary columns to match MongoDB schema
df.rename(columns={
    'date': 'DATE',
    'open_msft': 'OPEN',
    'high_msft': 'HIGH',
    'low_msft': 'LOW',
    'close': 'CLOSE',
    'volume_msft': 'VOLUME'
}, inplace=True)

# 5. Prepare for MongoDB Insertion

data_to_insert = df.to_dict('records')

for doc in data_to_insert:
    doc['Date'] = doc.pop('DATE')
    doc['Ticker'] = TICKER

# 6. Insert Data

collection.delete_many({"Ticker": TICKER})
result = collection.insert_many(data_to_insert) 

print(f"🎉 Successfully inserted {len(result.inserted_ids)} documents.")

# 7. Close Connection

client.close()