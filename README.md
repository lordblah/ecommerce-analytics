# E-Commerce Sales Analytics Pipeline

A complete data engineering project demonstrating modern data stack practices with **Dagster**, **dbt**, and **DuckDB**.

## 🎯 Project Overview

This project implements a production-ready analytics pipeline for e-commerce data using the **Medallion Architecture**:

- **Bronze Layer**: Raw data ingestion from CSV files
- **Silver Layer**: Cleaned, validated, and enriched data
- **Gold Layer**: Business metrics and analytics

### Key Features

✅ **Medallion Architecture** (Bronze → Silver → Gold)  
✅ **Incremental Loading** with dbt  
✅ **Data Quality Tests** at each layer  
✅ **Customer Segmentation** (RFM Analysis)  
✅ **Product Performance Analytics**  
✅ **Revenue Trends** with moving averages  
✅ **Interactive Dashboard** with Streamlit  
✅ **Asset Lineage** visualization in Dagster  

## 🏗️ Architecture

```
CSV Files → Dagster Assets → DuckDB (Bronze) 
    ↓
dbt Transformations → DuckDB (Silver)
    ↓
dbt Analytics → DuckDB (Gold)
    ↓
Streamlit Dashboard
```

## 📊 Data Model

### Bronze Layer
- `customers` - Raw customer data
- `products` - Raw product catalog
- `orders` - Raw order headers
- `order_items` - Raw order line items

### Silver Layer
- `silver_customers` - Cleaned customer records
- `silver_products` - Validated products
- `silver_orders` - Orders with calculated totals
- `silver_order_items` - Line items with enrichments

### Gold Layer
- `gold_customer_metrics` - Customer LTV, RFM scores, segments
- `gold_product_performance` - Sales, revenue, margins
- `gold_daily_revenue` - Time-series revenue metrics
- `gold_cohort_analysis` - Retention analysis

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ecommerce-analytics.git
cd ecommerce-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install the project
pip install -e .
```

### Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

This creates:
- 1,000 customers
- 200 products
- 5,000 orders
- ~15,000 order items

### Initialize dbt

```bash
cd dbt_project
dbt deps
dbt parse  # Generates manifest.json for Dagster
cd ..
```

### Run the Pipeline

#### Option 1: Dagster UI (Recommended)

```bash
# Start Dagster web server
dagster dev -f dagster_project/__init__.py
```

Open your browser to `http://localhost:3000` and:
1. Navigate to "Assets"
2. Click "Materialize all" to run the entire pipeline
3. View asset lineage and execution history

#### Option 2: Command Line

```bash
# Run dbt directly
cd dbt_project
dbt run
dbt test
```

### Launch Dashboard

```bash
streamlit run dashboard/streamlit_app.py
```

Open `http://localhost:8501` to view the interactive dashboard.

## 📁 Project Structure

```
ecommerce-analytics/
├── dagster_project/         # Dagster orchestration
│   ├── assets/
│   │   ├── bronze_assets.py # Data ingestion
│   │   └── dbt_assets.py    # dbt integration
│   └── __init__.py
├── dbt_project/             # dbt transformations
│   ├── models/
│   │   ├── bronze/          # Source views
│   │   ├── silver/          # Cleaned data
│   │   └── gold/            # Analytics
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/
│   ├── bronze/              # Raw CSV files
│   └── ecommerce.duckdb     # DuckDB database
├── dashboard/
│   └── streamlit_app.py     # Analytics dashboard
├── scripts/
│   └── generate_sample_data.py
├── requirements.txt
└── README.md
```

## 🔄 Pipeline Execution

### Manual Execution

1. **Bronze Layer**: Ingest CSV files into DuckDB
   ```bash
   # In Dagster UI, materialize bronze assets
   ```

2. **Silver Layer**: Clean and validate data
   ```bash
   cd dbt_project
   dbt run --models silver.*
   ```

3. **Gold Layer**: Generate analytics
   ```bash
   dbt run --models gold.*
   ```

### Scheduled Execution

The pipeline is configured to run daily at 2 AM:

```python
# In dagster_project/__init__.py
daily_pipeline_schedule = ScheduleDefinition(
    job=daily_sales_pipeline,
    cron_schedule="0 2 * * *",
)
```

Enable the schedule in the Dagster UI under "Automation" → "Schedules".

## 📈 Analytics & Metrics

### Customer Analytics
- **RFM Segmentation**: Champions, Loyal, At Risk, Lost
- **Lifetime Value (LTV)**: Total revenue per customer
- **Customer Lifecycle**: Active, Cooling Down, At Risk, Churned
- **Cohort Analysis**: Retention over time

### Product Analytics
- **Performance Tiers**: Top/Strong/Average/Under performers
- **Profitability**: Margins and profit per product
- **Inventory Status**: Stock levels and reorder points
- **Category Performance**: Revenue by product category

### Revenue Analytics
- **Daily Revenue**: With 7-day and 30-day moving averages
- **Growth Metrics**: Day/Week/Month over comparisons
- **Order Trends**: Volume and average order value
- **Seasonality**: Day of week patterns

## 🧪 Testing

### dbt Tests

Run data quality tests:

```bash
cd dbt_project
dbt test
```

Tests include:
- Uniqueness of primary keys
- Not null constraints
- Referential integrity
- Accepted values for status fields
- Custom business logic tests

### Generate dbt Documentation

```bash
cd dbt_project
dbt docs generate
dbt docs serve
```

## 🔧 Configuration

### DuckDB Connection

Edit `dbt_project/profiles.yml`:

```yaml
ecommerce:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../data/ecommerce.duckdb
      schema: main
```

### Dagster Resources

Resources are defined in `dagster_project/__init__.py`:

```python
resources={
    "dbt": DbtCliResource(project_dir="dbt_project")
}
```

## 📊 Dashboard Features

The Streamlit dashboard includes:

- **KPI Cards**: Orders, customers, revenue, AOV
- **Revenue Trends**: Interactive time-series charts
- **Customer Segmentation**: RFM analysis visualizations
- **Top Customers**: Leaderboard by lifetime value
- **Product Performance**: Revenue and margin analysis
- **Inventory Insights**: Stock status monitoring
- **Day-of-Week Analysis**: Seasonal patterns

## 🛠️ Development

### Adding New Models

1. Create SQL file in `dbt_project/models/`
2. Add tests in `schema.yml`
3. Run `dbt parse` to update manifest
4. Materialize in Dagster UI

### Adding New Assets

1. Create Python file in `dagster_project/assets/`
2. Define assets with `@asset` decorator
3. Reload Dagster definitions
4. Materialize new assets

## 🎓 Learning Outcomes

This project demonstrates:

- **Data Pipeline Orchestration** with Dagster
- **SQL Transformations** with dbt
- **Analytical SQL**: Window functions, CTEs, aggregations
- **Data Modeling**: Star schema, slowly changing dimensions
- **Data Quality**: Testing and validation
- **Analytics Engineering**: Medallion architecture
- **Business Intelligence**: Metrics and KPIs
- **Data Visualization**: Interactive dashboards

## 📚 Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Streamlit Documentation](https://docs.streamlit.io/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `dbt test`
5. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details

## 👤 Author

Your Name - [@yourusername](https://github.com/yourusername)

## 🙏 Acknowledgments

- Inspired by modern data stack best practices
- Built with open-source tools
- Community-driven development

---

**Ready to analyze your e-commerce data? Star ⭐ this repo if you find it useful!**