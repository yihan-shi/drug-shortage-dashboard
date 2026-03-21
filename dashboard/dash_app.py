import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
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


def normalize_single_source(x):
    if x == 1 or x == 1.0:
        return 'Single Source'
    elif x == 0 or x == 0.0:
        return 'Multiple Sources'
    else:
        return 'Unknown'


def load_characteristics_data():
    try:
        result = supabase.table('mart_shortage_characteristics').select('*').execute()
        df = pd.DataFrame(result.data)
        if not df.empty:
            df['first_update_date'] = pd.to_datetime(df['first_update_date'])
            df['last_update_date'] = pd.to_datetime(df['last_update_date'])
            df['single_source'] = df['single_source'].apply(normalize_single_source)
            df['route_category'] = df['route_category'].fillna('unknown')
        return df
    except Exception as e:
        print(f"Error loading characteristics data: {e}", file=sys.stderr)
        return pd.DataFrame()


def load_survival_data():
    try:
        result = supabase.table('mart_shortage_survival').select('*').execute()
        df = pd.DataFrame(result.data)
        if not df.empty:
            df['duration_days'] = pd.to_numeric(df['duration_days'], errors='coerce')
            # Drop rows with invalid durations
            df = df[df['duration_days'] > 0].copy()
            df['single_source'] = df['single_source'].apply(normalize_single_source)
            df['route_category'] = df['route_category'].fillna('unknown')
        return df
    except Exception as e:
        print(f"Error loading survival data: {e}", file=sys.stderr)
        return pd.DataFrame()


def kaplan_meier(durations, events):
    """Compute Kaplan-Meier survival curve. Returns (times, survival_probs)."""
    df = pd.DataFrame({'t': durations, 'e': events}).sort_values('t')
    times = sorted(df['t'].unique())
    n = len(df)
    surv = 1.0
    km_times = [0]
    km_surv = [1.0]
    for t in times:
        at_risk = n
        events_at_t = int(df[(df['t'] == t) & (df['e'] == True)].shape[0])
        censored_at_t = int(df[(df['t'] == t) & (df['e'] == False)].shape[0])
        if at_risk > 0 and events_at_t > 0:
            surv *= (1 - events_at_t / at_risk)
        km_times.append(t)
        km_surv.append(surv)
        n -= (events_at_t + censored_at_t)
    return km_times, km_surv


# Load data
print("Loading data...", file=sys.stderr)
chars_df = load_characteristics_data()
survival_df = load_survival_data()
print(f"Loaded {len(chars_df)} characteristics, {len(survival_df)} survival rows", file=sys.stderr)

# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Layout
app.layout = html.Div(className='container', children=[
    # Pie Chart
    html.Div(className='section', children=[
        html.H2("Shortage Drug Characteristics"),
        html.Div(style={'display': 'flex', 'gap': '20px', 'marginBottom': '20px', 'flexWrap': 'wrap'}, children=[
            html.Div(children=[
                html.Label("View By"),
                dcc.Dropdown(
                    id='pie-chart-category',
                    options=[
                        {'label': 'Route Category', 'value': 'route_category'},
                        {'label': 'Single Source', 'value': 'single_source'}
                    ],
                    value='route_category',
                    clearable=False,
                    style={'width': '300px', 'fontSize': '13px'}
                )
            ]),
            html.Div(children=[
                html.Label("Date Range"),
                dcc.DatePickerRange(
                    id='pie-date-range',
                    start_date=str(chars_df['first_update_date'].min().date()) if not chars_df.empty else None,
                    end_date=str(chars_df['last_update_date'].max().date()) if not chars_df.empty else None,
                    min_date_allowed=str(chars_df['first_update_date'].min().date()) if not chars_df.empty else None,
                    max_date_allowed=str(chars_df['last_update_date'].max().date()) if not chars_df.empty else None,
                    display_format='YYYY-MM-DD'
                )
            ])
        ]),
        dcc.Graph(id='pie-chart')
    ]),

    # Survival (KM) Chart
    html.Div(className='section', children=[
        html.H2("Shortage Resolution Survival Curve (Kaplan-Meier)"),
        html.Div(style={'display': 'flex', 'gap': '20px', 'marginBottom': '20px', 'flexWrap': 'wrap'}, children=[
            html.Div(children=[
                html.Label("Group By"),
                dcc.Dropdown(
                    id='km-group-by',
                    options=[
                        {'label': 'Route Category', 'value': 'route_category'},
                        {'label': 'Single Source', 'value': 'single_source'}
                    ],
                    value='route_category',
                    clearable=False,
                    style={'width': '300px', 'fontSize': '13px'}
                )
            ]),
            html.Div(children=[
                html.Label("Max Days"),
                dcc.Input(
                    id='km-max-days',
                    type='number',
                    value=1500,
                    min=30,
                    max=5000,
                    step=30,
                    style={'width': '100px', 'fontSize': '13px'}
                )
            ])
        ]),
        dcc.Graph(id='km-chart')
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
        fig.add_annotation(text='No data available', xref='paper', yref='paper',
                           x=0.5, y=0.5, showarrow=False, font={'size': 16, 'color': '#666'})
        return fig

    filtered = chars_df.copy()
    if start_date and end_date:
        filtered = filtered[
            (filtered['last_update_date'] >= pd.Timestamp(start_date)) &
            (filtered['first_update_date'] <= pd.Timestamp(end_date))
        ]

    drug_counts = (
        filtered.groupby(category)['drug_identifier']
        .nunique()
        .reset_index(name='count')
        .sort_values('count', ascending=False)
    )

    if drug_counts.empty:
        fig = go.Figure()
        fig.add_annotation(text='No data matches your filter criteria', xref='paper', yref='paper',
                           x=0.5, y=0.5, showarrow=False, font={'size': 16, 'color': '#666'})
        return fig

    total = drug_counts['count'].sum()
    title = 'Route Category' if category == 'route_category' else 'Single Source Status'
    fig = px.pie(
        drug_counts,
        values='count',
        names=category,
        title=f'Shortage Drugs by {title} (n={total} unique drugs, excl. discontinued)',
        height=500
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(font={'size': 13}, margin={'l': 20, 'r': 20, 't': 60, 'b': 20})
    return fig


@callback(
    Output('km-chart', 'figure'),
    [Input('km-group-by', 'value'),
     Input('km-max-days', 'value')]
)
def update_km_chart(group_by, max_days):
    if survival_df.empty:
        fig = go.Figure()
        fig.add_annotation(text='No survival data available', xref='paper', yref='paper',
                           x=0.5, y=0.5, showarrow=False, font={'size': 16, 'color': '#666'})
        return fig

    max_days = max_days or 1500
    df = survival_df[survival_df['duration_days'] <= max_days].copy()

    fig = go.Figure()
    groups = sorted(df[group_by].unique())

    for group in groups:
        group_data = df[df[group_by] == group]
        if len(group_data) < 2:
            continue
        times, surv = kaplan_meier(group_data['duration_days'].values, group_data['resolved'].values)
        fig.add_trace(go.Scatter(
            x=times, y=surv,
            mode='lines',
            name=f'{group} (n={len(group_data)})',
            line={'shape': 'hv'}
        ))

    title_label = 'Route Category' if group_by == 'route_category' else 'Single Source Status'
    fig.update_layout(
        title=f'Time to Shortage Resolution by {title_label} (excl. discontinued)',
        xaxis_title='Days Since Shortage Start',
        yaxis_title='Probability Still in Shortage',
        yaxis_range=[0, 1.05],
        height=550,
        font={'size': 13},
        legend={'title': title_label},
        margin={'l': 60, 'r': 20, 't': 60, 'b': 60}
    )
    return fig


server = app.server

if __name__ == '__main__':
    app.run(debug=True, port=8050)
