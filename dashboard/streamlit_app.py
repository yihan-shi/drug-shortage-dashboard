import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
from supabase import create_client, Client

# Page config
st.set_page_config(
    page_title="Drug Shortage Dashboard", 
    page_icon="💊",
    layout="wide"
)

# Supabase connection (using secrets for deployment)
@st.cache_resource
def init_supabase():
    try:
        # Try to use Streamlit secrets first (for deployment)
        supabase_url = st.secrets["SUPABASE_URL"]
        supabase_key = st.secrets["SUPABASE_SERVICE_KEY"]
    except:
        # Fallback to environment variables (for local development)
        from dotenv import load_dotenv
        load_dotenv()
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    
    return create_client(supabase_url, supabase_key)

# Load data with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    supabase = init_supabase()
    
    try:
        # Load from combined view
        result = supabase.table('drug_shortages_combined').select('*').limit(1000).execute()
        df = pd.DataFrame(result.data)
        
        if not df.empty:
            # Convert date columns
            date_cols = ['update_date', 'change_date', 'date_discontinued']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Create episodes data
            episodes_df = create_episodes_data(df)
            
            # Create rankings data
            rankings_df = create_rankings_data(df)
            
            return episodes_df, rankings_df
        else:
            return pd.DataFrame(), pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def create_episodes_data(df):
    """Convert raw data to episodes format"""
    episodes = []
    
    for drug in df['generic_name'].unique():
        drug_data = df[df['generic_name'] == drug].sort_values('update_date')
        
        for i, row in drug_data.iterrows():
            next_row = drug_data[drug_data['update_date'] > row['update_date']].head(1)
            
            if not next_row.empty:
                end_date = next_row.iloc[0]['update_date']
            else:
                end_date = pd.Timestamp.now()
            
            episodes.append({
                'generic_name': row['generic_name'],
                'company_name': row['company_name'],
                'therapeutic_category': row['therapeutic_category'],
                'availability_status': row['availability_status'],
                'episode_start_date': row['update_date'],
                'episode_end_date': end_date,
                'duration_days': (end_date - row['update_date']).days
            })
    
    return pd.DataFrame(episodes)

def create_rankings_data(df):
    """Create rankings from raw data"""
    rankings = df.groupby(['generic_name', 'company_name', 'therapeutic_category']).agg({
        'availability_status': lambda x: (x == 'not available').sum(),
        'update_date': 'count'
    }).reset_index()
    
    rankings.columns = ['generic_name', 'company_name', 'therapeutic_category', 'not_available_count', 'total_updates']
    rankings['not_available_pct'] = (rankings['not_available_count'] / rankings['total_updates'] * 100).round(2)
    
    return rankings.sort_values('not_available_count', ascending=False)

# Main app
def main():
    st.title("💊 Drug Shortage Interactive Dashboard")
    st.markdown("Analyze drug availability patterns over time")
    
    # Load data
    with st.spinner("Loading data..."):
        episodes_df, rankings_df = load_data()
    
    if episodes_df.empty:
        st.error("No data available. Please check your database connection.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Drug selection
    all_drugs = sorted(episodes_df['generic_name'].unique())
    selected_drugs = st.sidebar.multiselect(
        "Select Drugs:", 
        all_drugs, 
        default=all_drugs[:10] if len(all_drugs) > 10 else all_drugs
    )
    
    # Grouping selection
    group_by = st.sidebar.selectbox(
        "Group Timeline By:",
        ["generic_name", "therapeutic_category", "company_name"],
        format_func=lambda x: x.replace('_', ' ').title()
    )
    
    # Date range
    if not episodes_df.empty:
        min_date = episodes_df['episode_start_date'].min()
        max_date = episodes_df['episode_end_date'].max()
        
        date_range = st.sidebar.date_input(
            "Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📅 Availability Timeline")
        
        # Filter data
        filtered_df = episodes_df[
            (episodes_df['generic_name'].isin(selected_drugs))
        ]
        
        if len(date_range) == 2:
            filtered_df = filtered_df[
                (filtered_df['episode_start_date'] >= pd.Timestamp(date_range[0])) &
                (filtered_df['episode_end_date'] <= pd.Timestamp(date_range[1]))
            ]
        
        if not filtered_df.empty:
            # Create Gantt chart
            fig = px.timeline(
                filtered_df,
                x_start="episode_start_date",
                x_end="episode_end_date",
                y=group_by,
                color="availability_status",
                color_discrete_map={
                    'not available': '#ff4444',
                    'limited availability': '#ff8800',
                    'available': '#44ff44',
                    'discontinued': '#888888'
                },
                title=f"Drug Availability Timeline (Grouped by {group_by.replace('_', ' ').title()})",
                height=600
            )
            
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title=group_by.replace('_', ' ').title()
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data matches your filter criteria")
    
    with col2:
        st.subheader("📊 Quick Stats")
        
        if not filtered_df.empty:
            total_drugs = filtered_df['generic_name'].nunique()
            avg_episodes = filtered_df.groupby('generic_name').size().mean()
            not_available_pct = (filtered_df['availability_status'] == 'not available').mean() * 100
            
            st.metric("Drugs Analyzed", total_drugs)
            st.metric("Avg Episodes per Drug", f"{avg_episodes:.1f}")
            st.metric("% Time Not Available", f"{not_available_pct:.1f}%")
    
    # Rankings section
    st.subheader("🏆 Drug Shortage Rankings")
    
    ranking_metric = st.selectbox(
        "Rank by:",
        ["not_available_count", "not_available_pct"],
        format_func=lambda x: "Days Not Available" if x == "not_available_count" else "Percentage Not Available"
    )
    
    if not rankings_df.empty:
        top_20 = rankings_df.head(20)
        
        fig_bar = px.bar(
            top_20,
            x=ranking_metric,
            y='generic_name',
            orientation='h',
            title=f"Top 20 Drugs by {ranking_metric.replace('_', ' ').title()}",
            color=ranking_metric,
            color_continuous_scale='Reds'
        )
        
        fig_bar.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=600
        )
        
        st.plotly_chart(fig_bar, use_container_width=True)
        
        # Show data table
        with st.expander("View Raw Data"):
            st.dataframe(top_20)

if __name__ == "__main__":
    main()