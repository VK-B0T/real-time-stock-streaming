# 📈 Real-Time Stock Price Streaming & Visualization

This is a **major project** designed to stream, store, and visualize real-time stock price data using the following technologies:

- **Kafka** for real-time data streaming
- **ClickHouse** for high-performance data storage
- **Streamlit** for interactive visualization (candlestick chart, volume chart, live price)
- **Power BI** (embedded) for advanced dashboarding

> Developed as part of my final year BCA program, with a focus on transitioning from Data Analyst to Data Engineer roles.

---

## 🚀 Project Architecture

            ┌────────────┐       ┌─────────────┐
            │  Producer  ├──────▶│   Kafka     │
            └────────────┘       └────┬────────┘
                                      │
                                 ┌────▼────┐
                                 │ Consumer│
                                 └────┬────┘
                                      │
                               ┌──────▼──────┐
                               │ ClickHouse  │
                               └────┬────────┘
                                    │
                            ┌───────▼────────┐
                            │   Streamlit    │
                            │ Visualization  │
                            └────────────────┘
---

## 🧱 Tech Stack

| Component     | Description                               |
|---------------|-------------------------------------------|
| **Kafka**     | Message broker for streaming stock data   |
| **ClickHouse**| Columnar DBMS for high-speed inserts/reads|
| **Streamlit** | Real-time frontend dashboard              |
| **Power BI**  | Embedded reporting and analysis dashboard |
| **Docker**    | Environment orchestration                 |

---

## 📂 Project Structure
```
streaming-stock-project/
│
├── docker-compose.yml
├── producer.py
├── consumer.py
│
├── app.py
├── config.py
├── data_processing.py
├── chart_utils.py
│
├── requirements.txt
└── README.md
```

## 🔄 1. docker-compose.yml
🔧 Purpose: Bootstraps your entire backend infra in one shot.

Spins up:
- Kafka: Message broker for streaming data
- ZooKeeper: Kafka's brain for coordination
- ClickHouse: High-performance DB for storing real-time stock data
- You just run docker compose up -d, and the whole backend is ready.

## 📡 2. producer.py
📈 Purpose: Simulates real-time stock market data.
- Uses yfinance to fetch live stock data every 10 seconds
- Pushes it into Kafka under a specific topic (e.g., stock-prices)

## 📥 3. consumer.py
🧩 Purpose: Kafka data ingester.
- Listens to the Kafka topic (published by producer.py)
- Inserts each message (stock price data) into ClickHouse

## 🎯 4. app.py
🖥️ Purpose: Streamlit dashboard UI — this is your project’s main face.

Renders:
- Candlestick Chart (last 50 candles, updates every 5 min)
- Volume Chart (same time window as candle, matching colors)
- Live Price Line Chart (updates every 10 seconds)
- Embedded Power BI dashboard
- Dropdown for switching between stock tickers

## ⚙️ 5. config.py
🛠️ Purpose: Central place for all settings.

Stores:
- Kafka server details
- ClickHouse connection info
- Stock ticker list
- Refresh intervals
Makes it easy to tune or scale the project later.

## 6. 🧮 data_processing.py
📊 Purpose: Handles aggregation and transformation logic.
- Fetches latest stock entries from ClickHouse
- Performs candlestick aggregation in Python (not SQL)

## 📊 7. chart_utils.py
🎨 Purpose: Handles all chart rendering logic for Streamlit.
- Uses libraries like plotly or matplotlib to render:
- Candlestick chart
- Volume chart
- Live line chart
- Keeps app.py clean by separating visuals from layout logic

## ✅ Summary (Why This Rocks)
This structure is modular, maintainable, and production-ready. It cleanly separates:
Backend streaming (producer.py, consumer.py)
Database orchestration (docker-compose.yml)
Data logic (data_processing.py, config.py)
Frontend (app.py, chart_utils.py)
Docs + setup (README.md, requirements.txt)
You've got a complete pipeline from data ingestion to real-time dashboarding, with proper separation of concerns.

---

## ⚙️ How to Run the Project

### 1. Clone the Repo
```
git clone https://github.com/VK-B0T/streaming-stock-project.git
cd streaming-stock-project

```

### 2. Start Kafka, ClickHouse (Docker)
```
docker compose up -d

```
If Kafka fails to start, manually restart its container:
``` 
docker restart <kafka-container-id>

```

### 3. Start Producer (Fetches Live Data Every 10s)
```
python producer.py

```

### 4. Start Consumer (Writes to ClickHouse)
```
python consumer.py

```

### 5. Launch Streamlit App
```
streamlit run app.py

```
---

### 📊 Streamlit Dashboard Features
> Candlestick Chart (Left):
Aggregated data for the last 50 entries. Updates every 5 minutes.

> Volume Chart (Right):
Matches candlestick colors and also updates every 5 minutes.

> Live Price Line Chart:
Auto-refreshes every 10 seconds to show the most recent prices.

> Stock Ticker Filter:
Dropdown to switch between different stocks.

> Embedded Power BI Report:
Shows pre-published dashboard with financial analysis.

---

### 🧠 Notes
- All candlestick aggregations are done in Python (not ClickHouse).
- Timestamps such as 9:15 AM and 3:15 PM are preserved for market open/close accuracy.
- ClickHouse is optimized for fast inserts and analytical reads — perfect for this use case.
- Kafka handles real-time flow; avoid skipping consumer cycles to prevent lag.

### 🔮 Future Enhancements
- Replace polling with WebSocket-based updates for lower latency.
- Add alert system for sudden price movements.
- Export historical data as CSV or Excel.
- Add Redis cache layer for performance boost.
