# uk-crime-analytics-databricks

UK crime data engineering and analytics platform built with Databricks, PySpark and Delta Lake.

The project follows a **Medallion Architecture**, transforming raw Police.uk data through three layers:

**Bronze → Silver → Gold → Analytics / Visualisation**

## Databricks Structure

Our current Bronze layer contains street-level crime, crime outcomes and stop-and-search data covering **August 2023 – July 2026**.

As the project develops, we'll introduce separate Silver and Gold schemas in Unity Catalog.

```text
workspace
│
├── uk_crime_bronze/                  [COMPLETE]
│   │
│   ├── Tables
│   │   ├── street_crime
│   │   ├── outcomes
│   │   ├── stop_search
│   │   └── ingestion_control
│   │
│   └── Volumes
│       └── raw_data_files/
│           ├── archive/
│           ├── crime/
│           │   └── backfill/
│           ├── outcomes/
│           │   └── backfill/
│           └── stop_search/
│               └── backfill/
│
├── uk_crime_silver/                  [tbd]
│   │
│   └── Tables
│       ├── street_crime_clean
│       ├── outcomes_clean
│       ├── stop_search_clean
│       └── geographic_reference
│
└── uk_crime_gold/                    [tbd]
    │
    └── Tables
        ├── monthly_crime_trends
        ├── crime_by_area
        ├── crime_outcome_summary
        └── stop_search_summary
```

The Silver and Gold table names above are suggestions for now. We can decide on the final tables together as we develop the project.

### Bronze — Raw Data

The Bronze layer has already been built using Python and PySpark.

The initial Police.uk historical backfill covers 36 months, with approximately **33.5 million records** across our three datasets.

Bronze stores the source data in Delta tables with standardised column names and ingestion metadata.

### Silver — Cleaning & Transformation

Silver is where we'll prepare the Bronze data for analysis.

This could include:

- Converting columns into appropriate data types
- Handling missing values and duplicates
- Standardising dates and crime categories
- Validating geographic coordinates
- Enriching data with geographic information
- Implementing data quality checks

The resulting tables should be clean, consistent and suitable for further analysis.

### Gold — Analytics

Gold will contain the final aggregated, analytics-ready datasets built from Silver.

Potential examples include:

- Monthly crime trends and changes over time
- Crime distribution by police force or geographic area
- Crime outcome statistics
- Stop-and-search patterns
- KPIs for our dashboard or application

We'll decide on the specific analytical questions and outputs together.

---

## Repository Structure

This is the proposed GitHub structure as we move into Silver and Gold.

```text
uk-crime-analytics-databricks/
│
├── src/
│   │
│   ├── ingestion/                    [EXISTING]
│   │   └── Bronze ingestion scripts
│   │
│   ├── silver/                       [tbd]
│   │   ├── clean_street_crime.py
│   │   ├── clean_outcomes.py
│   │   ├── clean_stop_search.py
│   │   └── data_quality.py
│   │
│   ├── gold/                         [tbd]
│   │   ├── crime_trends.py
│   │   ├── geographic_analysis.py
│   │   └── policing_outcomes.py
│   │
│   └── app/                          [potentially?]
│       └── Dashboard / application code
│
├── tests/                            [tbd]
│   └── Pipeline validation tests
│
├── .gitignore
└── README.md
```

These folders are a suggested structure, not a strict requirement. We can add and adjust them as we go.

The repository should contain **code, documentation and configuration only**. Raw Police.uk CSV/ZIP files should remain in Databricks, not GitHub.

---

## Pls Read! :)

The initial Police.uk backfill and Bronze ingestion have already been completed.

**You do not need to rerun the files in `src/ingestion`.**

The resulting Bronze tables are available in the original Databricks environment:

- `workspace.uk_crime_bronze.street_crime`
- `workspace.uk_crime_bronze.outcomes`
- `workspace.uk_crime_bronze.stop_search`

You'll need access to the same Databricks environment to query these tables. Connecting GitHub alone doesn't automatically share the data.

The ingestion scripts are kept in the repository as part of the pipeline code and for future development.

### When starting work

1. Pull the latest `main` branch.
2. Check that you can access the relevant Bronze tables in Databricks.
3. Create a new feature branch for your task.
4. Develop and test your work on that branch.
5. Commit and push your changes.
6. Open a Pull Request into `main` so we can review and merge the changes.

For example, branch names could be:

```text
feature/silver-crime-cleaning
feature/data-quality
feature/geographic-enrichment
feature/gold-analysis
feature/dashboard
```

Please avoid overwriting or dropping shared tables without checking with the group first. Git branches separate our code changes, but they don't create separate copies of Databricks tables.

We'll figure out the remaining tasks and divide up the work as we go!

thx ^^
