import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import sys
from supabase import create_client, Client, ClientOptions
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_ANON_KEY")
if not supabase_url or not supabase_key:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment variables")
supabase = create_client(
    supabase_url,
    supabase_key,
    options=ClientOptions(postgrest_client_timeout=30)
)


def load_characteristics_data():
    try:
        result = supabase.table('mart_shortage_characteristics').select('*').execute()
        chars_df = pd.DataFrame(result.data)
        if not chars_df.empty:
            chars_df['first_update_date'] = pd.to_datetime(chars_df['first_update_date'])
            chars_df['last_update_date'] = pd.to_datetime(chars_df['last_update_date'])
            # Normalize single_source to readable labels
            chars_df['single_source'] = chars_df['single_source'].apply(
                lambda x: 'Single Source' if x == 1 or x == 1.0 or str(x).lower() in ('true', 'yes', 't')
                else ('Multiple Sources' if x == 0 or x == 0.0 or str(x).lower() in ('false', 'no', 'f')
                       else 'Unknown')
            )
            # Fill missing route_category
            chars_df['route_category'] = chars_df['route_category'].fillna('unknown')
        return chars_df
    except Exception as e:
        print(f"Error loading characteristics data: {e}", file=sys.stderr)
        return pd.DataFrame()


# Load data
chars_df = load_characteristics_data()

# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Layout
app.layout = html.Div(className='container', children=[
    html.Div(className='section', children=[
        html.H2("Shortage Drug Characteristics", className='section-title'),
        html.Div(className='filters', children=[
            html.Div(className='filter-section', children=[
                html.Label("View By", className='filter-label'),
                dcc.Dropdown(
                    id='pie-chart-category',
                    options=[
                        {'label': 'Route Category (Injectable vs Other)', 'value': 'route_category'},
                        {'label': 'Single Source', 'value': 'single_source'}
                    ],
                    value='route_category',
                    clearable=False,
                    style={'fontSize': '13px', 'maxWidth': '400px'}
                )
            ]),
            html.Div(className='filter-section', children=[
                html.Label("Date Range", className='filter-label'),
                dcc.DatePickerRange(
                    id='pie-date-range',
                    start_date=str(chars_df['first_update_date'].min().date()) if not chars_df.empty else None,
                    end_date=str(chars_df['last_update_date'].max().date()) if not chars_df.empty else None,
                    min_date_allowed=str(chars_df['first_update_date'].min().date()) if not chars_df.empty else None,
                    max_date_allowed=str(chars_df['last_update_date'].max().date()) if not chars_df.empty else None,
                    display_format='YYYY-MM-DD',
                    style={'fontSize': '13px'}
                )
            ])
        ]),
        dcc.Graph(id='pie-chart', config={'displayModeBar': False})
    ])
])


@callback(
    Output('pie-chart', 'figure'),
    [Input('pie-chart-category', 'value'),
     Input('pie-date-range', 'start_date'),
     Input('pie-date-range', 'end_date')]
)
def update_pie_chart(category, start_date, end_date):
    if chars_df.empty:
        fig = go.Figure()
        fig.update_layout(
            xaxis={'visible': False}, yaxis={'visible': False},
            annotations=[{'text': 'No characteristics data available',
                          'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                          'font': {'size': 14, 'color': '#666'}}]
        )
        return fig

    filtered = chars_df.copy()
    if start_date and end_date:
        # Include drugs active within the selected date range
        filtered = filtered[
            (filtered['last_update_date'] >= pd.Timestamp(start_date)) &
            (filtered['first_update_date'] <= pd.Timestamp(end_date))
        ]

    # Count distinct drug_identifiers per category value
    drug_counts = (
        filtered.groupby(category)['drug_identifier']
        .nunique()
        .reset_index(name='count')
        .sort_values('count', ascending=False)
    )

    if drug_counts.empty:
        fig = go.Figure()
        fig.update_layout(
            xaxis={'visible': False}, yaxis={'visible': False},
            annotations=[{'text': 'No data matches your filter criteria',
                          'xref': 'paper', 'yref': 'paper', 'showarrow': False,
                          'font': {'size': 14, 'color': '#666'}}]
        )
        return fig

    title = 'Route Category' if category == 'route_category' else 'Single Source Status'
    fig = px.pie(
        drug_counts,
        values='count',
        names=category,
        title=f'Shortage Drugs by {title} (n={drug_counts["count"].sum()} unique drugs)',
        height=500
    )

    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font={'size': 12, 'color': '#1a1a1a'},
        margin={'l': 20, 'r': 20, 't': 60, 'b': 20}
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')

    return fig


# Expose server for deployment
server = app.server

if __name__ == '__main__':
    app.run(debug=True, port=8050)
