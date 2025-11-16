import plotly.graph_objects as go
import pandas as pd
from pymongo import MongoClient
import plotly.offline as pyo 
import os 

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "FinancialDataDB"
COLLECTION_NAME = "StockPrices"
TICKER = "MSFT"


OUTPUT_FOLDER = r"D:\CODE\MMA" 

# 1. Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
except Exception as e:
    print(f"❌ Could not connect to MongoDB. Error: {e}")
    exit()

collection = db[COLLECTION_NAME]

# 2. MongoDB Aggregation Pipeline for 50-Day SMA

pipeline = [
    {"$match": {"Ticker": TICKER}},
    {"$sort": {"Date": 1}},
    {"$setWindowFields": {
        "partitionBy": "$Ticker",
        "sortBy": {"Date": 1},
        "output": {
            "SMA_50": {
                "$avg": "$CLOSE",
                "window": {"documents": [-49, 0]} # 50-day window for SMA
            }
        }
    }},
    {"$project": {"Date": 1, "CLOSE": 1, "OPEN": 1, "HIGH": 1, "LOW": 1, "VOLUME": 1, "SMA_50": 1, "_id": 0}}
]

# 3. Execute Pipeline and Load into Pandas
print("Executing MongoDB Aggregation Pipeline and loading data...")
cursor = collection.aggregate(pipeline)
final_df = pd.DataFrame(list(cursor))

# Clean up the DataFrame
final_df.set_index('Date', inplace=True)
final_df.dropna(subset=['SMA_50'], inplace=True) 

print("\nFinal Data Head (showing calculated SMA):")
print(final_df.tail())

# 4. Create Candlestick Plot
fig = go.Figure(data=[
    # Candlestick Trace (OHLC Data)
    go.Candlestick(
        x=final_df.index,
        open=final_df['OPEN'],
        high=final_df['HIGH'],
        low=final_df['LOW'],
        close=final_df['CLOSE'],
        name='Daily Price'
    ),
    # Moving Average Trace (Line Data)
    go.Scatter(
        x=final_df.index,
        y=final_df['SMA_50'],
        line=dict(color='blue', width=2),
        name='50-Day SMA'
    )
])

# 5. Customize and Display Layout
fig.update_layout(
    title=f"MongoMarket Analyzer: Candlestick Chart with 50-Day SMA for {TICKER}",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    xaxis_rangeslider_visible=True,
    template="plotly_dark",
    height=800
)

# 6. Save and Show Results
file_path = os.path.join(OUTPUT_FOLDER, 'msft_analyzer_chart.html')

# Save the chart locally as a failsafe
pyo.plot(fig, filename=file_path, auto_open=False)
print(f"\n✅ Chart saved to: {file_path}")

# Try to open the chart in the browser
print("Attempting to open chart in browser...")
fig.show()

# 7. Close Connection
client.close()