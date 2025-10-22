import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import sys
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_ANON_KEY")
if not supabase_url or not supabase_key:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment variables")
supabase = create_client(supabase_url, supabase_key)

# Constants
SHORTAGE_STATUS_COLORS = {
    'new': '#ff4444',
    'continued': '#ff8800',
    'available': '#44ff44',
    'discontinued': '#888888'
}

# Load data
def load_data():
    try:
        result = supabase.table('drug_shortage_episodes').select('*').execute()
        episodes_df = pd.DataFrame(result.data)

        if not episodes_df.empty:
            # Convert date columns
            episodes_df['episode_start_date'] = pd.to_datetime(episodes_df['episode_start_date'])
            episodes_df['episode_end_date'] = pd.to_datetime(episodes_df['episode_end_date'])

            # Create rankings from episodes data
            rankings_df = episodes_df.groupby(['generic_name', 'company_name', 'therapeutic_category']).agg({
                'episode_duration_days': ['sum', 'count'],
                'shortage_status': lambda x: (x.isin(['new', 'continued'])).sum()
            }).reset_index()

            rankings_df.columns = ['generic_name', 'company_name', 'therapeutic_category',
                        'total_days', 'total_episodes', 'shortage_episodes']

            shortage_summary = episodes_df[
                episodes_df['shortage_status'].isin(['new', 'continued'])
            ].groupby(['generic_name', 'therapeutic_category'])['episode_duration_days'].sum().reset_index()

            shortage_summary = shortage_summary.rename(
                columns={'episode_duration_days': 'shortage_days'}
            )

            rankings_df = rankings_df.merge(
                shortage_summary,
                on=['generic_name', 'therapeutic_category'],
                how='left'
            )

            rankings_df['shortage_days'] = rankings_df['shortage_days'].fillna(0)
            rankings_df['shortage_pct'] = (rankings_df['shortage_days'] / rankings_df['total_days'] * 100).round(2)
            rankings_df = rankings_df.sort_values('shortage_days', ascending=False)

            return episodes_df, rankings_df
        else:
            return pd.DataFrame(), pd.DataFrame()

    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr)
        return pd.DataFrame(), pd.DataFrame()

# Load data
episodes_df, rankings_df = load_data()

# Helper functions
def stat_card(label, value):
    return html.Div(className='stat-card', children=[
        html.Div(label, className='stat-label'),
        html.Div(value, className='stat-value')
    ])

# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Get all drugs for dropdown
all_drugs = sorted(episodes_df['generic_name'].unique()) if not episodes_df.empty else []

# Default to 10 most recently active drugs
if not episodes_df.empty:
    recent_drugs = episodes_df.sort_values('episode_end_date', ascending=False)['generic_name'].unique()[:10]
    default_drugs = list(recent_drugs)
else:
    default_drugs = []

# Get date range
if not episodes_df.empty:
    min_date = episodes_df['episode_start_date'].min()
    max_date = episodes_df['episode_end_date'].max()
else:
    min_date = max_date = None

# Layout
app.layout = html.Div(className='container', children=[
    # Header
    html.H1("Drug Shortage Dashboard"),
    html.Div("Analyze drug shortage patterns over time", className='subtitle'),

    # Filters
    html.Div(className='filters', children=[
        html.Div(className='filter-section', children=[
            html.Label("Select Drugs", className='filter-label'),
            dcc.Dropdown(
                id='drug-selector',
                options=[{'label': drug, 'value': drug} for drug in all_drugs],
                value=default_drugs,
                multi=True,
                placeholder="Select drugs to analyze...",
                style={'fontSize': '13px'}
            )
        ]),
        html.Div(className='filter-section', children=[
            html.Label("Group Timeline By", className='filter-label'),
            dcc.Dropdown(
                id='group-by-selector',
                options=[
                    {'label': 'Generic Name', 'value': 'generic_name'},
                    {'label': 'Therapeutic Category', 'value': 'therapeutic_category'},
                    {'label': 'Company Name', 'value': 'company_name'}
                ],
                value='generic_name',
                clearable=False,
                style={'fontSize': '13px'}
            )
        ]),
        html.Div(className='filter-section', children=[
            html.Label("Date Range", className='filter-label'),
            dcc.DatePickerRange(
                id='date-range',
                start_date=min_date,
                end_date=max_date,
                min_date_allowed=min_date,
                max_date_allowed=max_date,
                display_format='YYYY-MM-DD',
                style={'fontSize': '13px'}
            )
        ])
    ]),

    # Stats
    html.Div(id='stats-section', className='stats-grid'),

    # Timeline Chart
    html.Div(className='section', children=[
        html.H2("Shortage Timeline", className='section-title'),
        dcc.Graph(id='timeline-chart', config={'displayModeBar': False})
    ]),

    # Rankings
    html.Div(className='section', children=[
        html.H2("Drug Rankings", className='section-title'),
        html.Div(style={'marginBottom': '16px'}, children=[
            html.Label("Rank by", className='filter-label'),
            dcc.Dropdown(
                id='ranking-metric',
                options=[
                    {'label': 'Days In Shortage', 'value': 'shortage_days'},
                    {'label': 'Percentage In Shortage', 'value': 'shortage_pct'}
                ],
                value='shortage_days',
                clearable=False,
                style={'fontSize': '13px', 'maxWidth': '300px'}
            )
        ]),
        dcc.Graph(id='ranking-chart', config={'displayModeBar': False})
    ])
])

# Callbacks
@callback(
    [Output('stats-section', 'children'),
     Output('timeline-chart', 'figure'),
     Output('ranking-chart', 'figure')],
    [Input('drug-selector', 'value'),
     Input('group-by-selector', 'value'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('ranking-metric', 'value')]
)
def update_dashboard(selected_drugs, group_by, start_date, end_date, ranking_metric):
    # Filter episodes data
    filtered_df = episodes_df[episodes_df['generic_name'].isin(selected_drugs)]

    if start_date and end_date:
        filtered_df = filtered_df[
            (filtered_df['episode_end_date'] >= pd.Timestamp(start_date)) &
            (filtered_df['episode_start_date'] <= pd.Timestamp(end_date))
        ]

    # Calculate stats
    if not filtered_df.empty:
        total_drugs = filtered_df['generic_name'].nunique()
        avg_episodes = filtered_df.groupby('generic_name').size().mean()

        # Calculate percentage of time in shortage (weighted by duration)
        total_days = filtered_df['episode_duration_days'].sum()
        shortage_days = filtered_df[filtered_df['shortage_status'].isin(['new', 'continued'])]['episode_duration_days'].sum()
        shortage_pct = (shortage_days / total_days * 100) if total_days > 0 else 0

        stats = html.Div(className='stats-grid', children=[
            stat_card("Drugs Analyzed", f"{total_drugs}"),
            stat_card("Avg Episodes per Drug", f"{avg_episodes:.1f}"),
            stat_card("% Time In Shortage", f"{shortage_pct:.1f}%")
        ])
    else:
        stats = html.Div("No data available", style={'color': '#666', 'textAlign': 'center', 'padding': '20px'})

    # Timeline chart
    if not filtered_df.empty:
        timeline_fig = px.timeline(
            filtered_df,
            x_start="episode_start_date",
            x_end="episode_end_date",
            y=group_by,
            color="shortage_status",
            color_discrete_map=SHORTAGE_STATUS_COLORS,
            height=500
        )

        timeline_fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font={'size': 12, 'color': '#1a1a1a'},
            xaxis={'title': 'Date', 'gridcolor': '#f0f0f0'},
            yaxis={'title': group_by.replace('_', ' ').title(), 'gridcolor': '#f0f0f0'},
            legend={'title': {'text': 'Status'}, 'orientation': 'h', 'y': -0.15},
            margin={'l': 20, 'r': 20, 't': 20, 'b': 80},
            autosize=True
        )

        # Make chart responsive
        timeline_fig.update_xaxes(automargin=True)
        timeline_fig.update_yaxes(automargin=True)
    else:
        timeline_fig = go.Figure()
        timeline_fig.update_layout(
            xaxis={'visible': False},
            yaxis={'visible': False},
            annotations=[{
                'text': 'No data matches your filter criteria',
                'xref': 'paper',
                'yref': 'paper',
                'showarrow': False,
                'font': {'size': 14, 'color': '#666'}
            }]
        )

    # Rankings chart
    if not rankings_df.empty:
        top_20 = rankings_df.head(20)

        ranking_fig = px.bar(
            top_20,
            x=ranking_metric,
            y='generic_name',
            orientation='h',
            color=ranking_metric,
            color_continuous_scale='Reds',
            height=600
        )

        ranking_fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font={'size': 12, 'color': '#1a1a1a'},
            xaxis={'title': ranking_metric.replace('_', ' ').title(), 'gridcolor': '#f0f0f0'},
            yaxis={'title': '', 'categoryorder': 'total ascending', 'gridcolor': '#f0f0f0'},
            showlegend=False,
            margin={'l': 150, 'r': 20, 't': 20, 'b': 20},
            autosize=True
        )

        # Make chart responsive
        ranking_fig.update_xaxes(automargin=True)
        ranking_fig.update_yaxes(automargin=True)
    else:
        ranking_fig = go.Figure()
        ranking_fig.update_layout(
            xaxis={'visible': False},
            yaxis={'visible': False},
            annotations=[{
                'text': 'No ranking data available',
                'xref': 'paper',
                'yref': 'paper',
                'showarrow': False,
                'font': {'size': 14, 'color': '#666'}
            }]
        )

    return stats, timeline_fig, ranking_fig

# Expose server for deployment
server = app.server

if __name__ == '__main__':
    app.run(debug=True, port=8050)
