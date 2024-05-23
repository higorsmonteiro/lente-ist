import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import numpy as np
from dash_echarts import DashECharts

# Sample data
df = pd.DataFrame({
    "Date": pd.date_range(start="2023-01-01", periods=100, freq="D"),
    "Value": np.random.randn(100).cumsum()
})

# Initialize the Dash app with Bootstrap stylesheet and custom CSS
external_stylesheets = [
    dbc.themes.FLATLY,
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    '/assets/custom.css'  # Ensure this path is correct
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)

# ECharts options
echarts_options = {
    'title': {
        'text': 'ECharts Line Chart'
    },
    'tooltip': {
        'trigger': 'axis'
    },
    'xAxis': {
        'type': 'category',
        'data': df['Date'].astype(str).tolist()
    },
    'yAxis': {
        'type': 'value'
    },
    'series': [{
        'data': df['Value'].tolist(),
        'type': 'line'
    }]
}

# Define the layout of the app
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Img(src="/assets/logo.png", style={'width': '100%'}),
                html.H2("Dashboard", className='text-center'),
                html.P("This is an introductory text for the dashboard.", className='text-center')
            ], style={
                'position': 'fixed', 'top': 0, 'left': 0, 'width': '250px',
                'height': '100%', 'background-color': '#f8f9fa', 'padding': '20px',
                'overflow-y': 'auto', 'border-right': '1px solid #ddd'
            })
        ], width=2),
        dbc.Col([
            dbc.Row([
                dbc.Col(html.H1("Dash Plotly and ECharts Example", className='text-center text-primary mb-4'), width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Tabs(id='tabs-example', value='tab-1', children=[
                        dcc.Tab(label='Tab 1', value='tab-1', className='custom-tab', selected_className='custom-tab--selected'),
                        dcc.Tab(label='Tab 2', value='tab-2', className='custom-tab', selected_className='custom-tab--selected'),
                        dcc.Tab(label='Tab 3', value='tab-3', className='custom-tab', selected_className='custom-tab--selected'),
                    ], className='custom-tabs')
                ], width={'size': 4}, className='tabs-row')
            ], justify='start'),
            html.Div(id='tabs-content-example')
        ], width=10)
    ])
], fluid=True, className='mt-4 mb-4 dash-container')

# Define callback to update tab content
@app.callback(
    Output('tabs-content-example', 'children'),
    Input('tabs-example', 'value')
)
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([
            dbc.Row([
                dbc.Col(dcc.Graph(id='line-chart', style={'width': '100%'}), width=8),
                dbc.Col(dbc.Card(
                    dbc.CardBody([
                        html.H4("Caption", className="card-title"),
                        html.P("This is the caption for the line chart.", className="card-text")
                    ])
                ), width=4)
            ], justify='center', style={'marginBottom': '40px'})
        ], style={'marginBottom': '40px'})
    elif tab == 'tab-2':
        return html.Div([
            dbc.Row([
                dbc.Col(dcc.Graph(
                    figure=px.bar(df, x='Date', y='Value', title='Bar Chart Example'),
                    style={'width': '100%'}
                ), width=8),
                dbc.Col(dbc.Card(
                    dbc.CardBody([
                        html.H4("Caption", className="card-title"),
                        html.P("This is the caption for the bar chart.", className="card-text")
                    ])
                ), width=4)
            ], justify='center', style={'marginBottom': '40px'})
        ], style={'marginBottom': '40px'})
    elif tab == 'tab-3':
        return html.Div([
            dbc.Row([
                dbc.Col(DashECharts(
                    option=echarts_options,
                    id='echarts-line-chart',
                    style={'height': '500px', 'width': '100%'}
                ), width=8),
                dbc.Col(dbc.Card(
                    dbc.CardBody([
                        html.H4("Caption", className="card-title"),
                        html.P("This is the caption for the ECharts line chart.", className="card-text")
                    ])
                ), width=4)
            ], justify='center', style={'marginBottom': '40px'})
        ], style={'marginBottom': '40px'})

# Define callback to update graph based on selected date range
@app.callback(
    Output('line-chart', 'figure'),
    Input('tabs-example', 'value'),
    Input('line-chart', 'relayoutData')
)
def update_line_chart(tab, relayoutData):
    if tab == 'tab-1':
        filtered_df = df.copy()
        if relayoutData and 'xaxis.range' in relayoutData:
            start_date = pd.to_datetime(relayoutData['xaxis.range'][0])
            end_date = pd.to_datetime(relayoutData['xaxis.range'][1])
            filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        fig = px.line(filtered_df, x='Date', y='Value', title='Value Over Time')
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(
                    visible=True
                ),
                type="date"
            ),
            paper_bgcolor='#f9f9f9',
            plot_bgcolor='#f9f9f9',
            font_color='#333'
        )
        return fig
    return {}

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
