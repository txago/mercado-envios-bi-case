# Synthetic Data Generation

This script generates realistic inbound operations data that mimics Mercado Livre's actual logistics patterns.

## 🎯 Purpose

Since real order data is classified, we simulate:
- Regional seller distributions (60% SP, 25% MG, etc.)
- Warehouse capacity variations  
- Realistic delivery time patterns
- Operational status distributions

## 📈 Data Realism Features

- **Geographic Distribution**: Mirrors actual Brazilian e-commerce patterns
- **Time-based Congestion**: Peak hours (8AM, 11AM, 12PM) have higher delays
- **Status Realism**: 75% delivered, with realistic delay distributions
- **Capacity Constraints**: Warehouses have varying throughput capacities

## 🚀 Usage

```bash
python generate_synthetic_data.py
```

The script will generate 3 CSV files in the project root, which should then be moved to the seeds/ folder for dbt processing.