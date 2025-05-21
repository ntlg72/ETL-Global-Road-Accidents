import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import requests
import plotly.graph_objects as go

# Inicializar la app
app = dash.Dash(__name__)
app.title = "Real-Time Accident Dashboard"

# Lista para almacenar los datos acumulados
accumulated_data = []

# Layout
app.layout = html.Div(style={'fontFamily': 'Arial', 'backgroundColor': '#fafafa', 'padding': '20px'}, children=[
    html.H1("🚦 Real-Time Accident Dashboard", style={'textAlign': 'center', 'color': '#222'}),

    html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'flex-start'}, children=[
        html.Div(id='metrics-container', style={
            'width': '20%',
            'display': 'flex',
            'flexDirection': 'column',
            'gap': '20px',
            'padding': '10px'
        }),

        html.Div([
            dcc.Graph(id='vehicles-line-chart', style={'width': '90%', 'height': '600px'})  # Tamaño grande
        ], style={
            'width': '80%',
            'display': 'flex',
            'justifyContent': 'center',  # Centrar horizontalmente
            'alignItems': 'center',      # Centrar verticalmente
            'paddingLeft': '20px'
        })
    ]),

    dcc.Interval(id='interval-component', interval=2000, n_intervals=0),  # Cambiado a 2 segundos (2000 ms)
    dcc.Store(id='data-store', data=[])  # Almacenamiento para los datos
])

# Callback
@app.callback(
    [Output('metrics-container', 'children'),
     Output('vehicles-line-chart', 'figure'),
     Output('data-store', 'data')],
    [Input('interval-component', 'n_intervals')],
    [State('data-store', 'data')]
)
def update_dashboard(n, stored_data):
    global accumulated_data
    try:
        response = requests.get("http://localhost:8000/data")
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list) or len(data) == 0:
            raise ValueError("La API no devolvió una lista de datos válida.")

        df = pd.DataFrame(data)

        # Convertir campos numéricos de texto a número
        numeric_columns = [
            "number_of_vehicles_involved",
            "speed_limit",
            "number_of_injuries",
            "number_of_fatalities",
            "emergency_response_time",
            "traffic_volume",
            "pedestrians_involved",
            "cyclists_involved",
            "population_density"
        ]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # Convertir fecha
        df["event_time"] = pd.to_datetime(df["event_time"], unit='ms')

        # Añadir nuevos datos a la lista acumulada
        accumulated_data.extend(data)
        
        # Mantener solo los últimos 50 datos
        accumulated_data = accumulated_data[-50:]

        # Crear DataFrame con los datos acumulados
        df_accumulated = pd.DataFrame(accumulated_data)
        df_accumulated[numeric_columns] = df_accumulated[numeric_columns].apply(pd.to_numeric, errors='coerce')
        df_accumulated["event_time"] = pd.to_datetime(df_accumulated["event_time"], unit='ms')

        # KPIs basados en los datos actuales (no acumulados)
        total_accidents = len(df)
        total_fatalities = df['number_of_fatalities'].sum()
        total_cyclists = df['cyclists_involved'].sum()
        total_injuries = df['number_of_injuries'].sum()
        total_pedestrians = df['pedestrians_involved'].sum()

        kpi_cards = [
            create_kpi("🚗", "Number of accidents", total_accidents),
            create_kpi("🪦", "Number of fatalities", int(total_fatalities)),
            create_kpi("🚴", "Number of cyclists involved", int(total_cyclists)),
            create_kpi("🧑‍⚕️", "Number of injuries", int(total_injuries)),
            create_kpi("🚶", "Number of pedestrians involved", int(total_pedestrians)),
        ]

        # Línea de tiempo con datos acumulados
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_accumulated['event_time'],
            y=df_accumulated['number_of_vehicles_involved'],
            mode='lines+markers',
            line=dict(color='purple'),
            marker=dict(size=6),
            name='Vehicles Involved'
        ))
        fig.update_layout(
            title="Number of vehicles involved by time of accident",
            xaxis_title="Time of accident",
            yaxis_title="Number of vehicles involved",
            plot_bgcolor="#fff",
            paper_bgcolor="#fafafa",
            font=dict(color="#333"),
            margin=dict(l=40, r=20, t=60, b=40)
        )

        return kpi_cards, fig, accumulated_data

    except Exception as e:
        error_msg = f"Error fetching or processing data: {e}"
        return [html.Div(error_msg, style={'color': 'red'})], go.Figure(), accumulated_data

# Función para tarjeta de KPI
def create_kpi(emoji, title, value):
    return html.Div(style={
        'backgroundColor': 'white',
        'padding': '15px',
        'borderRadius': '8px',
        'boxShadow': '0 2px 6px rgba(0,0,0,0.1)',
        'textAlign': 'center'
    }, children=[
        html.Div(emoji, style={'fontSize': '30px'}),
        html.Div(title, style={'fontSize': '16px', 'marginTop': '5px', 'color': '#666'}),
        html.H2(f"{value}", style={'margin': '5px 0', 'color': '#111'})
    ])

if __name__ == '__main__':
    app.run(debug=True)

