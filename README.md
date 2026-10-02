# uk-crime-analytics-databricks
UK crime data engineering and analytics platform built with Databricks

## Databricks Structure

The project currently uses:

```text
workspace
│
└── uk_crime_bronze
    │
    ├── Tables
    │   ├── street_crime
    │   └── ingestion_control
    │
    └── Volumes
        └── raw_data_files
            ├── archive/
            └── crime/
                └── backfill/
```

Additional Silver and Gold schemas will be introduced as the project develops.

---

## Repository Structure

```text
uk-crime-analytics-databricks/
│
├── src/
│   └── ingestion/
│       └── ingestion pipeline code
│
├── .gitignore
└── README.md
```

The repository contains **code, documentation and configuration only**.

Large ZIP files, CSV files and other raw datasets should remain in Databricks and must not be committed to GitHub.
