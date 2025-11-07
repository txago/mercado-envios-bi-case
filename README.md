# Mercado Envios BI Case - Inbound Optimization

A data transformation pipeline for analyzing and optimizing Mercado Livre's inbound scheduling operations.

## 📁 Project Structure

```
MERCADO_ENVIOS_BI_CASE/
├── models/
│ ├── staging/ # Raw data cleaning and standardization
│ │ └── stg_inbound_operations.sql
│ └── marts/ # Business-ready data models
│ ├── mart_inbound_kpis.sql
│ ├── mart_forecast_slots.sql
│ └── schema.yml # dbt documentation and tests
├── seeds/ # Source CSV data
│ ├── bt_fbm_inbound_operations_agg.csv
│ ├── lk_sf_commercial_sellers_data.csv
│ └── lk_shp_facilities.csv
├── analyses/ # Ad-hoc analysis queries
├── macros/ # Reusable SQL snippets
├── scripts/ # Synthetic data generation scripts
│ └── generate_synthetic_data.py
└── dbt_project.yml # Project configuration
```

## 🚀 Quick Start

1. **Generate Synthetic Data** (if needed):
```bash
cd scripts
python generate_synthetic_data.py
```
2. **Load Seed Data**:
```bash
dbt seed
```
3. **Run Models**:
```bash
dbt run
```
4. **Generate Documentation**:
```bash
dbt docs generate
dbt docs serve
```

## 📊 Data Models

### Staging Layer
- `stg_inbound_operations`: Cleansed inbound operations with calculated metrics
### Mart Layer
- `mart_inbound_kpis`: Key performance indicators by site/hour/day
- `mart_forecast_slots`: Demand forecasting for resource planning

## 🔗 Connected Systems

* **Data Warehouse**: Google BigQuery
* **Data Source**: Synthetic CSVs (simulating Mercado Livre API)
* **BI Tool**: Looker Studio

## 📈 Typical Analysis Workflow

1. **Data Refresh**: Regenerate synthetic data if needed
2. **Pipeline Run**: `dbt run` to process new data
3. **Dashboard Update**: Looker automatically pulls latest data
4. **Performance Review**: Monitor KPIs in dashboard
5. **Forecast Planning**: Use mart_forecast_slots for resource allocation

## 🛠️ Development

- Add new metrics: Modify `stg_inbound_operations.sql`
- Create new KPIs: Add to `mart_inbound_kpis.sql` 
- Forecast adjustments: Update `mart_forecast_slots.sql`