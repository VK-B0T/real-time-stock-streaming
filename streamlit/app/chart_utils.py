import plotly.graph_objects as go

# Candlestick Chart
def create_candlestick_chart(df):
    df["Time Bin"] = df["Time Bin"].astype(str)  # Convert datetime to string for better x-axis handling
    print('CHarts',df["Time Bin"])
    return go.Figure(data=[
        go.Candlestick(
            x=df["Time Bin"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            increasing_line_color='green',
            decreasing_line_color='red'
        )
    ])

# Volume Chart
def create_volume_chart(df):
    return go.Figure(data=[
        go.Bar(
            x=df["Time Bin"],
            y=df["Volume"],
            marker_color=["green" if close >= open else "red" for open, close in zip(df["Open"], df["Close"])]
        )
    ])

# Live Price Line Chart
def create_live_price_chart(df):
    return go.Figure(data=[
        go.Scatter(
            x=df["Formatted_Timestamp"],
            y=df["Current_Price"],
            mode='lines+markers',
            line=dict(color="blue")
        )
    ])
