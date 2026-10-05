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


##Pls read!

The initial Police.uk backfill and Bronze ingestion have already been run.

**You do not need to rerun the files in `src/ingestion`.**

The resulting Bronze tables are available in our shared Databricks environment:

- `workspace.uk_crime_bronze.street_crime`
- `workspace.uk_crime_bronze.outcomes`
- `workspace.uk_crime_bronze.stop_search`

The ingestion scripts are kept in the repository as part of the pipeline code and for future development.

When working on the project/starting:

1. Pull the latest `main` branch.
2. Check that you can query the Bronze tables in Databricks
3. Create a new feature branch for your task
4. Develop and test your work on that branch
5. Push your changes and open a Pull Request into `main`

Please avoid overwriting or dropping shared tables without checking with the group first.

thx 
