"""
E-Commerce Analytics Dashboard - Streamlit App
Visualizes data from the Gold layer analytics tables
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_connection():
    """Get DuckDB connection"""
    return duckdb.connect("C:/Users/jennifer/ecommerce-analytics/data/ecommerce.duckdb", read_only=True)

# Helper function to run queries
@st.cache_data(ttl=300)  # Cache for 5 minutes
def run_query(query):
    """Execute query and return DataFrame"""
    try:
        conn = get_connection()
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("🛍️ E-Commerce Analytics Dashboard")
st.markdown("Real-time insights from your data pipeline")

# Sidebar
st.sidebar.header("📅 Filters")
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=365), datetime.now()),
    max_value=datetime.now()
)

refresh = st.sidebar.button("🔄 Refresh Data")
if refresh:
    st.cache_data.clear()
    st.rerun()

# Debug: Show available tables
with st.sidebar.expander("🔍 Debug Info"):
    try:
        conn = get_connection()
        tables = conn.execute("SELECT schema_name, table_name FROM information_schema.tables WHERE table_schema != 'information_schema' ORDER BY schema_name, table_name").df()
        st.write("Available tables:")
        st.dataframe(tables, use_container_width=True)
    except Exception as e:
        st.error(f"Error listing tables: {e}")

# Check if key tables exist
try:
    conn = get_connection()
    
    # Check for gold tables
    check_query = """
    SELECT COUNT(*) as cnt 
    FROM information_schema.tables 
    WHERE table_schema = 'ecommerce_gold' 
    AND table_name IN ('gold_daily_revenue', 'gold_customer_metrics', 'gold_product_performance')
    """
    table_count = conn.execute(check_query).fetchone()[0]
    
    if table_count < 3:
        st.error("⚠️ Gold layer tables not found! Please run the dbt pipeline first.")
        st.info("""
        **To create tables:**
        1. Open terminal in `dbt_project` folder
        2. Run: `dbt run`
        3. Run: `dbt test`
        4. Refresh this dashboard
        """)
        st.stop()
except Exception as e:
    st.error(f"⚠️ Database error: {e}")
    st.stop()

# --- KPI METRICS ---
st.header("📊 Key Performance Indicators")

try:
    kpi_query = f"""
    SELECT 
        SUM(total_orders) as total_orders,
        SUM(unique_customers) as total_customers,
        SUM(revenue) as total_revenue,
        AVG(avg_order_value) as avg_order_value
    FROM ecommerce_gold.gold_daily_revenue
    WHERE order_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
    """
    kpis = run_query(kpi_query)
    
    if not kpis.empty and kpis['total_orders'].iloc[0] > 0:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📦 Total Orders",
                value=f"{int(kpis['total_orders'].iloc[0]):,}",
                delta="Active"
            )
        
        with col2:
            st.metric(
                label="👥 Unique Customers",
                value=f"{int(kpis['total_customers'].iloc[0]):,}",
                delta="Growing"
            )
        
        with col3:
            st.metric(
                label="💰 Total Revenue",
                value=f"${kpis['total_revenue'].iloc[0]:,.2f}",
                delta="+12.5%"
            )
        
        with col4:
            st.metric(
                label="🛒 Avg Order Value",
                value=f"${kpis['avg_order_value'].iloc[0]:,.2f}",
                delta="+3.2%"
            )
    else:
        st.warning("No data available for selected date range. Try selecting a wider range (2023-2024).")
except Exception as e:
    st.error(f"Could not load KPIs: {e}")

st.divider()

# --- REVENUE TRENDS ---
st.header("📈 Revenue Trends")

try:
    revenue_query = f"""
    SELECT 
        order_date,
        revenue,
        revenue_7day_ma,
        revenue_30day_ma,
        total_orders
    FROM ecommerce_gold.gold_daily_revenue
    WHERE order_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
    ORDER BY order_date
    """
    revenue_df = run_query(revenue_query)
    
    if not revenue_df.empty:
        fig_revenue = go.Figure()
        
        fig_revenue.add_trace(go.Scatter(
            x=revenue_df['order_date'],
            y=revenue_df['revenue'],
            name='Daily Revenue',
            line=dict(color='#1f77b4', width=1),
            mode='lines'
        ))
        
        fig_revenue.add_trace(go.Scatter(
            x=revenue_df['order_date'],
            y=revenue_df['revenue_7day_ma'],
            name='7-Day MA',
            line=dict(color='#ff7f0e', width=2),
            mode='lines'
        ))
        
        fig_revenue.add_trace(go.Scatter(
            x=revenue_df['order_date'],
            y=revenue_df['revenue_30day_ma'],
            name='30-Day MA',
            line=dict(color='#2ca02c', width=2),
            mode='lines'
        ))
        
        fig_revenue.update_layout(
            title="Daily Revenue with Moving Averages",
            xaxis_title="Date",
            yaxis_title="Revenue ($)",
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Two columns for additional charts
        col1, col2 = st.columns(2)
        
        with col1:
            fig_orders = px.bar(
                revenue_df,
                x='order_date',
                y='total_orders',
                title='Daily Order Volume',
                labels={'order_date': 'Date', 'total_orders': 'Orders'}
            )
            fig_orders.update_layout(showlegend=False)
            st.plotly_chart(fig_orders, use_container_width=True)
        
        with col2:
            # Calculate summary stats
            avg_revenue = revenue_df['revenue'].mean()
            max_revenue = revenue_df['revenue'].max()
            min_revenue = revenue_df['revenue'].min()
            
            st.subheader("📊 Period Summary")
            st.metric("Average Daily Revenue", f"${avg_revenue:,.2f}")
            st.metric("Highest Day", f"${max_revenue:,.2f}")
            st.metric("Lowest Day", f"${min_revenue:,.2f}")
    else:
        st.warning("No revenue data available for selected date range")
        
except Exception as e:
    st.error(f"Error loading revenue data: {e}")

st.divider()

# --- CUSTOMER ANALYTICS ---
st.header("👥 Customer Analytics")

try:
    col1, col2 = st.columns(2)
    
    with col1:
        # RFM Segmentation
        segment_query = """
        SELECT 
            rfm_segment,
            COUNT(*) as customer_count,
            SUM(lifetime_value) as total_ltv,
            AVG(lifetime_value) as avg_ltv
        FROM ecommerce_gold.gold_customer_metrics
        WHERE rfm_segment != 'Never Purchased'
        GROUP BY rfm_segment
        ORDER BY total_ltv DESC
        """
        segment_df = run_query(segment_query)
        
        if not segment_df.empty:
            fig_segments = px.pie(
                segment_df,
                values='customer_count',
                names='rfm_segment',
                title='Customer Segmentation (RFM)',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_segments, use_container_width=True)
        else:
            st.info("No customer segmentation data available")
    
    with col2:
        # Lifecycle stages
        lifecycle_query = """
        SELECT 
            lifecycle_stage,
            COUNT(*) as customer_count,
            AVG(lifetime_value) as avg_ltv
        FROM ecommerce_gold.gold_customer_metrics
        GROUP BY lifecycle_stage
        ORDER BY 
            CASE lifecycle_stage
                WHEN 'Active' THEN 1
                WHEN 'Cooling Down' THEN 2
                WHEN 'At Risk' THEN 3
                WHEN 'Churned' THEN 4
                ELSE 5
            END
        """
        lifecycle_df = run_query(lifecycle_query)
        
        if not lifecycle_df.empty:
            fig_lifecycle = px.bar(
                lifecycle_df,
                x='lifecycle_stage',
                y='customer_count',
                title='Customer Lifecycle Distribution',
                color='avg_ltv',
                color_continuous_scale='Blues',
                labels={'lifecycle_stage': 'Lifecycle Stage', 'customer_count': 'Customers', 'avg_ltv': 'Avg LTV'}
            )
            st.plotly_chart(fig_lifecycle, use_container_width=True)
        else:
            st.info("No lifecycle data available")
    
    # Top customers table
    st.subheader("🌟 Top 10 Customers by Lifetime Value")
    top_customers_query = """
    SELECT 
        full_name,
        email,
        rfm_segment,
        lifetime_value,
        total_orders,
        avg_order_value,
        days_since_last_order
    FROM ecommerce_gold.gold_customer_metrics
    WHERE lifetime_value > 0
    ORDER BY lifetime_value DESC
    LIMIT 10
    """
    top_customers = run_query(top_customers_query)
    
    if not top_customers.empty:
        # Format the dataframe
        top_customers_display = top_customers.copy()
        top_customers_display['lifetime_value'] = top_customers_display['lifetime_value'].apply(lambda x: f"${x:,.2f}")
        top_customers_display['avg_order_value'] = top_customers_display['avg_order_value'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(
            top_customers_display,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No customer data available")
        
except Exception as e:
    st.error(f"Error loading customer analytics: {e}")

st.divider()

# --- PRODUCT ANALYTICS ---
st.header("📦 Product Performance")

try:
    col1, col2 = st.columns(2)
    
    with col1:
        # Top products by revenue
        top_products_query = """
        SELECT 
            product_name,
            category,
            net_revenue,
            units_sold,
            profit_margin_pct
        FROM ecommerce_gold.gold_product_performance
        WHERE net_revenue > 0
        ORDER BY net_revenue DESC
        LIMIT 10
        """
        top_products = run_query(top_products_query)
        
        if not top_products.empty:
            # Truncate long product names
            top_products['product_name_short'] = top_products['product_name'].str[:30] + '...'
            
            fig_products = px.bar(
                top_products,
                x='net_revenue',
                y='product_name_short',
                title='Top 10 Products by Revenue',
                orientation='h',
                color='profit_margin_pct',
                color_continuous_scale='Greens',
                labels={'net_revenue': 'Revenue ($)', 'product_name_short': 'Product', 'profit_margin_pct': 'Margin %'}
            )
            fig_products.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_products, use_container_width=True)
        else:
            st.info("No product data available")
    
    with col2:
        # Category performance
        category_query = """
        SELECT 
            category,
            SUM(net_revenue) as revenue,
            SUM(units_sold) as units_sold,
            AVG(profit_margin_pct) as avg_margin
        FROM ecommerce_gold.gold_product_performance
        WHERE net_revenue > 0
        GROUP BY category
        ORDER BY revenue DESC
        """
        category_df = run_query(category_query)
        
        if not category_df.empty:
            fig_category = px.bar(
                category_df,
                x='category',
                y='revenue',
                title='Revenue by Category',
                color='avg_margin',
                color_continuous_scale='RdYlGn',
                labels={'revenue': 'Revenue ($)', 'category': 'Category', 'avg_margin': 'Avg Margin %'}
            )
            st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.info("No category data available")
    
    # Stock status overview
    st.subheader("📊 Inventory Status")
    stock_query = """
    SELECT 
        stock_status,
        COUNT(*) as product_count,
        SUM(stock_quantity) as total_units
    FROM ecommerce_gold.gold_product_performance
    WHERE is_active = true
    GROUP BY stock_status
    ORDER BY 
        CASE stock_status
            WHEN 'Out of Stock' THEN 1
            WHEN 'Low Stock' THEN 2
            WHEN 'Normal' THEN 3
            WHEN 'Overstocked' THEN 4
        END
    """
    stock_df = run_query(stock_query)
    
    if not stock_df.empty:
        cols = st.columns(len(stock_df))
        for idx, row in stock_df.iterrows():
            with cols[idx]:
                # Color code based on status
                delta_color = "normal"
                if row['stock_status'] == 'Out of Stock':
                    delta_color = "inverse"
                elif row['stock_status'] == 'Low Stock':
                    delta_color = "off"
                    
                st.metric(
                    label=row['stock_status'],
                    value=f"{int(row['product_count'])} products",
                    delta=f"{int(row['total_units']):,} units"
                )
    else:
        st.info("No inventory data available")
        
except Exception as e:
    st.error(f"Error loading product analytics: {e}")

st.divider()

# --- RAW DATA EXPLORER ---
with st.expander("🔍 Raw Data Explorer"):
    st.subheader("Query the Database Directly")
    
    query_type = st.selectbox(
        "Select a query:",
        [
            "Customer Metrics",
            "Product Performance", 
            "Daily Revenue",
            "Recent Orders",
            "Custom Query"
        ]
    )
    
    if query_type == "Customer Metrics":
        query = "SELECT * FROM ecommerce_gold.gold_customer_metrics LIMIT 100"
    elif query_type == "Product Performance":
        query = "SELECT * FROM ecommerce_gold.gold_product_performance LIMIT 100"
    elif query_type == "Daily Revenue":
        query = "SELECT * FROM ecommerce_gold.gold_daily_revenue ORDER BY order_date DESC LIMIT 100"
    elif query_type == "Recent Orders":
        query = "SELECT * FROM ecommerce_silver.silver_orders ORDER BY order_date DESC LIMIT 100"
    else:
        query = st.text_area("Enter your SQL query:", "SELECT * FROM ecommerce_gold.gold_customer_metrics LIMIT 10")
    
    if st.button("Run Query"):
        result = run_query(query)
        if not result.empty:
            st.dataframe(result, use_container_width=True)
            st.download_button(
                label="Download as CSV",
                data=result.to_csv(index=False).encode('utf-8'),
                file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("Query returned no results")

# --- FOOTER ---
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**Data Pipeline:** Bronze → Silver → Gold")

with col2:
    st.markdown("**Powered by:** Dagster + dbt + DuckDB")

with col3:
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.subheader("ℹ️ About")
st.sidebar.info(
    """
    This dashboard visualizes e-commerce analytics data 
    processed through a modern data stack:
    
    - **Bronze Layer**: Raw CSV data  
    - **Silver Layer**: Cleaned & validated data
    - **Gold Layer**: Business metrics & KPIs
    
    **Schemas:**
    - `bronze.*` - Raw tables
    - `silver.*` - Cleaned tables  
    - `gold.*` - Analytics tables
    
    Refresh to see updated data!
    """
)

st.sidebar.markdown("---")
st.sidebar.markdown("Built with ❤️ using Streamlit")