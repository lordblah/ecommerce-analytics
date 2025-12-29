# Getting Started Guide

This guide will walk you through setting up and running the E-Commerce Analytics Pipeline from scratch.

## 📋 Prerequisites

Before you begin, ensure you have:

- **Python 3.9 or higher** installed
- **Git** installed
- **Basic SQL knowledge** (helpful but not required)
- **10GB of free disk space** (for dependencies and data)

## 🚀 Step-by-Step Setup

### Step 1: Clone and Setup Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/ecommerce-analytics.git
cd ecommerce-analytics

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

### Step 2: Install Dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# Install the project in development mode
pip install -e .

# Verify installation
dagster --version
dbt --version
```

### Step 3: Generate Sample Data

```bash
# Run the data generation script
python scripts/generate_sample_data.py

# You should see output like:
# Creating 1,000 customers...
# Creating 200 products...
# Creating 5,000 orders...
# Creating order items...
# 
# DATA GENERATION COMPLETE
# Customers: 1,000
# Products: 200
# Orders: 5,000
# Order Items: 15,234
```

### Step 4: Initialize dbt

```bash
# Navigate to dbt project
cd dbt_project

# Install dbt packages (if any)
dbt deps

# Parse the project to generate manifest.json
dbt parse

# Test the connection
dbt debug

# Go back to root directory
cd ..
```

### Step 5: Run the Pipeline

#### Option A: Using Dagster UI (Recommended for Beginners)

```bash
# Start the Dagster web server
dagster dev -f dagster_project/__init__.py
```

This will:
- Start a web server on `http://localhost:3000`
- Open your browser automatically (or navigate manually)

In the Dagster UI:

1. **View Assets**: Click "Assets" in the left sidebar
2. **See Lineage**: View the data flow from Bronze → Silver → Gold
3. **Materialize All**: Click "Materialize all" button to run the entire pipeline
4. **Monitor Progress**: Watch assets turn green as they complete
5. **Check Logs**: Click on any asset to see execution details

#### Option B: Using dbt CLI Directly

```bash
# Navigate to dbt project
cd dbt_project

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Step 6: Launch the Dashboard

```bash
# Make sure you're in the root directory
cd ..

# Start Streamlit
streamlit run dashboard/streamlit_app.py
```

The dashboard will open at `http://localhost:8501`

## 🎯 What to Expect

### After Running the Pipeline

You should see:

1. **Bronze Tables** in DuckDB:
   - `bronze.customers`
   - `bronze.products`
   - `bronze.orders`
   - `bronze.order_items`

2. **Silver Tables** in DuckDB:
   - `silver.silver_customers`
   - `silver.silver_products`
   - `silver.silver_orders`

3. **Gold Tables** in DuckDB:
   - `gold.gold_customer_metrics`
   - `gold.gold_product_performance`
   - `gold.gold_daily_revenue`

### Dashboard Features

The dashboard displays:

- **KPIs**: Total orders, customers, revenue, AOV
- **Revenue Trends**: Daily revenue with moving averages
- **Customer Segmentation**: RFM analysis
- **Top Customers**: By lifetime value
- **Product Performance**: Revenue and margins
- **Inventory Status**: Stock levels

## 🔍 Exploring the Data

### Query DuckDB Directly

```bash
# Open DuckDB CLI
duckdb data/ecommerce.duckdb

# Run queries
SELECT * FROM gold.gold_customer_metrics LIMIT 10;
SELECT * FROM gold.gold_daily_revenue ORDER BY order_date DESC LIMIT 30;

# Exit
.exit
```

### Using Python

```python
import duckdb

# Connect to database
conn = duckdb.connect('data/ecommerce.duckdb')

# Run query
result = conn.execute("""
    SELECT 
        rfm_segment,
        COUNT(*) as customers,
        AVG(lifetime_value) as avg_ltv
    FROM gold.gold_customer_metrics
    GROUP BY rfm_segment
    ORDER BY avg_ltv DESC
""").df()

print(result)
```

## 🔄 Running Updates

### Incremental Loads

The pipeline supports incremental loading:

```bash
cd dbt_project

# Run only changed models
dbt run --select state:modified+

# Run a specific model
dbt run --select silver_orders

# Run tests for specific model
dbt test --select silver_orders
```

### Scheduling

To run the pipeline on a schedule:

1. Open Dagster UI at `http://localhost:3000`
2. Navigate to "Automation" → "Schedules"
3. Find "daily_pipeline_schedule"
4. Toggle it ON
5. The pipeline will run daily at 2 AM

## 🐛 Troubleshooting

### Common Issues

#### "Module not found" errors

```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

#### "No such file: manifest.json"

```bash
cd dbt_project
dbt parse
cd ..
```

#### Database locked errors

```bash
# Close all DuckDB connections
# Delete the .wal file
rm data/ecommerce.duckdb.wal
```

#### Dagster not starting

```bash
# Check if port 3000 is in use
# On macOS/Linux:
lsof -i :3000

# On Windows:
netstat -ano | findstr :3000

# Kill the process or use a different port
dagster dev -f dagster_project/__init__.py -p 3001
```

## 📚 Next Steps

### Customization Ideas

1. **Add More Metrics**:
   - Create new dbt models in `dbt_project/models/gold/`
   - Example: cohort analysis, product recommendations

2. **Enhance Dashboard**:
   - Add new visualizations to `dashboard/streamlit_app.py`
   - Create filters for date ranges, categories, etc.

3. **Connect Real Data**:
   - Replace CSV generation with API calls
   - Connect to your database or data warehouse

4. **Add Tests**:
   - Create custom dbt tests in `dbt_project/tests/`
   - Add data quality checks

5. **Deploy to Production**:
   - Set up Dagster Cloud or self-hosted instance
   - Configure production database
   - Set up monitoring and alerts

### Learning Resources

- **Dagster**: [dagster.io/learn](https://dagster.io/learn)
- **dbt**: [courses.getdbt.com](https://courses.getdbt.com)
- **DuckDB**: [duckdb.org/docs](https://duckdb.org/docs)
- **SQL**: Practice with the sample data!

## 💡 Tips

1. **Start Small**: Run one layer at a time to understand the flow
2. **Check Logs**: Always review execution logs in Dagster
3. **Test Often**: Run `dbt test` after making changes
4. **Document**: Add descriptions to your models in `schema.yml`
5. **Version Control**: Commit your changes frequently

## 🎓 Understanding the Pipeline

### Data Flow

```
1. CSV Files (Raw Data)
   ↓
2. Dagster Bronze Assets (Load to DuckDB)
   ↓
3. dbt Silver Models (Clean & Validate)
   ↓
4. dbt Gold Models (Analytics)
   ↓
5. Streamlit Dashboard (Visualize)
```

### Key Concepts

- **Assets**: Dagster's representation of data
- **Materializing**: Running the code to produce an asset
- **Models**: dbt's SQL transformations
- **Tests**: Validation rules for data quality
- **Lineage**: Visual representation of data dependencies

## 📞 Getting Help

- **Issues**: Open an issue on GitHub
- **Discussions**: Join the discussions tab
- **Documentation**: Check the README.md
- **Community**: Dagster and dbt Slack communities

## ✅ Success Checklist

- [ ] Virtual environment activated
- [ ] Dependencies installed
- [ ] Sample data generated
- [ ] dbt parsed successfully
- [ ] Pipeline runs without errors
- [ ] Dashboard loads and displays data
- [ ] Can query DuckDB directly
- [ ] Understand the data flow

**Congratulations! You've successfully set up the E-Commerce Analytics Pipeline! 🎉**

---

Need help? Check the troubleshooting section or open an issue on GitHub.