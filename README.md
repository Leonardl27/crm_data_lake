# CRM Data Lake

A data engineering portfolio project demonstrating a complete data lake architecture with ETL pipelines, data quality checks, and interactive visualizations.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  External APIs  │────▶│    QA Layer     │────▶│   PROD Layer    │────▶│  Visualization  │
│  - RandomUser   │     │  - Raw data     │     │  - Cleaned data │     │  - GitHub Pages │
│  - JSONPlace    │     │  - Validation   │     │  - Standardized │     │  - Chart.js     │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Features

- **Two-Layer Data Architecture**: QA (staging) and PROD (production) layers
- **Data Quality Checks**: Null detection, duplicate checks, schema validation
- **Automated ETL**: GitHub Actions workflow for scheduled data updates
- **Interactive Dashboard**: Chart.js visualizations on GitHub Pages

## Project Structure

```
crm_data_lake/
├── .github/workflows/    # GitHub Actions for automated ETL
├── config/               # Configuration settings
├── data/
│   ├── qa/              # QA layer (raw/staging data)
│   └── prod/            # PROD layer (validated data)
├── docs/                # GitHub Pages dashboard
├── pipelines/
│   ├── extract/         # Data extraction scripts
│   ├── transform/       # Data transformation & promotion
│   └── quality_checks/  # Data validation
└── requirements.txt
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the Full Pipeline

```bash
python pipelines/run_pipeline.py
```

This will:
1. Extract customer data from RandomUser API
2. Extract interaction data from JSONPlaceholder API
3. Run quality checks on the data
4. Promote validated data to PROD layer
5. Generate dashboard data for visualization

### 3. View the Dashboard

Open `docs/index.html` in your browser, or deploy to GitHub Pages.

## Data Sources

| Source | Description | Data Type |
|--------|-------------|-----------|
| [RandomUser API](https://randomuser.me/) | Realistic fake customer profiles | Customers |
| [JSONPlaceholder](https://jsonplaceholder.typicode.com/) | Posts and comments | Interactions |

## Quality Checks

The pipeline includes the following quality validations:

- **Required Fields**: Ensures all mandatory fields are present
- **Null Percentage**: Validates null values are below threshold
- **Duplicate Detection**: Identifies and flags duplicate records
- **Email Validation**: Checks email format validity
- **Data Type Enforcement**: Ensures correct data types

## GitHub Actions

The ETL pipeline runs automatically:
- **On Push**: When pipeline code changes
- **Scheduled**: Daily at 6 AM UTC
- **Manual**: Via workflow dispatch

## Technologies

- **Python 3.11+**: ETL pipelines
- **Pandas**: Data manipulation
- **Pydantic**: Data validation
- **Chart.js**: Interactive charts
- **GitHub Actions**: CI/CD automation
- **GitHub Pages**: Dashboard hosting

## License

MIT
