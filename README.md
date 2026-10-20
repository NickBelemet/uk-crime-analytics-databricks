# uk-crime-analytics-databricks

A UK crime data engineering and analytics platform built on Databricks. The project ingests open police data from [police.uk](https://data.police.uk/), enriches it with ONS population estimates and Index of Deprivation (IoD 2019) data, and produces business-ready gold tables for dashboard analytics.

---

## Architecture

The pipeline follows a three-layer medallion architecture:

```text
                    Bronze                    Silver                    Gold
                 (raw ingest)           (clean + dedup)        (aggregations + enrichment)

  police.uk CSVs ──► street_crime ──► street_crime ──► street_crime_summary
  (36 months)       outcomes        outcomes         outcomes_summary
                    stop_search     stop_search      stop_search_summary
                                                      force_enrichment
  ONS XLSX ────────► population_lsoa ──► population_lsoa ──► (joins into force_enrichment)
  (mid-2022/24)

  IoD 2019 CSV ────► deprivation_lsoa ──► deprivation_lsoa ──► (joins into force_enrichment)
```

### Data Sources

| Source | Description | Format | Rows | Refresh |
| --- | --- | --- | --- | --- |
| police.uk | Street crime, outcomes, stop & search | CSV archives | ~18M / ~14M / ~1.4M | Monthly |
| ONS | LSOA mid-year population estimates (2022–2024) | XLSX | 107K (35,672 LSOAs × 3 years) | Annual (~June) |
| IoD 2019 | English Indices of Deprivation (File 7) | CSV | 32,844 LSOAs | Periodic (2019 vintage) |

---

## Databricks Structure

```text
workspace
│
├── uk_crime_bronze                          ── Raw ingestion layer
│   ├── street_crime                          (17.8M rows, 36 months, 44 forces)
│   ├── outcomes                              (13.5M rows)
│   ├── stop_search                            (1.39M rows)
│   ├── population_lsoa                        (107K rows, ONS mid-2022/23/24)
│   ├── deprivation_lsoa                       (32,844 rows, IoD 2019 all domains)
│   └── raw_data_files/                        (UC volume — raw source files)
│       ├── crime/backfill/                    police.uk CSV archives
│       ├── archive/                           ZIP bundles
│       └── exploratory/                       ONS + IoD source files
│
├── uk_crime_silver                           ── Cleaned + validated layer
│   ├── street_crime                          type-cast, deduped, geo-validated
│   ├── outcomes                               joined to street_crime for crime_type
│   ├── stop_search                            type-cast, month derived from date
│   ├── population_lsoa                        deduped by lsoa_code + year
│   └── deprivation_lsoa                       14 cols: IMD + 7 domain scores
│
└── uk_crime_gold                             ── Business-ready analytics layer
    ├── street_crime_summary                  force × month × crime_type × lsoa_code
    │                                         6.0M rows, 36 partitions, 99.8% coords
    ├── outcomes_summary                      force × month × crime_type × outcome_type × lsoa_code
    │                                         6.9M rows, 36 partitions, 99.6% coords
    ├── stop_search_summary                    force × month × object_of_search × grid
    │                                         444K rows, 37 partitions, 98.7% coords
    └── force_enrichment                       43 forces × population + deprivation
                                              60.3M population, weighted IMD scores
```

### Gold Table Details

**street_crime_summary** — crime counts at force × month × crime type × LSOA grain, with LSOA centroid coordinates for map visualisations. Month-partitioned for automation-ready incremental updates.

**outcomes_summary** — outcome counts at force × month × crime type × outcome type × LSOA grain. Crime type is joined from street_crime via crime_id. Month-partitioned.

**stop_search_summary** — search counts at force × month × object of search × ~1.1km coordinate grid (lat/lon rounded to 2dp). Includes outcome breakdowns (arrest, summons, etc.) and demographic breakdowns (gender, age buckets). Month-partitioned.

**force_enrichment** — force-level socioeconomic context for dashboard joins. Aggregates ONS population (2024 vintage) and IoD 2019 deprivation to police force areas via LSOA mapping. Population-weighted averages for IMD and 7 domain scores. Not month-partitioned (static, refreshed annually).

| Column | Description |
| --- | --- |
| `force_id` | Police force identifier |
| `total_population` | Sum of LSOA populations (2024 estimates) |
| `lsoa_count` | Number of LSOAs mapped to this force |
| `lsoa_with_deprivation` | LSOAs with IoD data (89% match — 2011 vs 2021 boundary changes) |
| `avg_imd_score` | Population-weighted average IMD score |
| `avg_imd_rank` | Average IMD rank (unweighted) |
| `avg_imd_decile` | Average IMD decile (1 = most deprived, 10 = least) |
| `avg_income_score` | Population-weighted income deprivation score |
| `avg_employment_score` | Population-weighted employment deprivation score |
| `avg_education_score` | Population-weighted education deprivation score |
| `avg_health_score` | Population-weighted health deprivation score |
| `avg_crime_score` | Population-weighted crime domain score |
| `avg_barriers_score` | Population-weighted barriers to housing score |
| `avg_living_env_score` | Population-weighted living environment score |

---

## Repository Structure

This is the proposed GitHub structure as we move into Silver and Gold.

```text
uk-crime-analytics-databricks/
│
├── src/
│   ├── ingestion/
│   │   ├── bronze_crime.py                  Street crime bronze ingestion
│   │   ├── bronze_outcomes_stop_search.py   Outcomes + stop/search bronze ingestion
│   │   ├── bronze_population.py             ONS LSOA population (stdlib XLSX parser)
│   │   ├── extract_outcomes_stop_search.py   Extraction helpers
│   │   ├── extract_street.py                 Extraction helpers
│   │   └── ...                               Early exploration scripts
│   │
│   ├── silver/
│   │   ├── street_crime_clean_silver.py     Cleaning + dedup + validation
│   │   ├── outcomes_clean_silver.py         Cleaning + crime_type join
│   │   └── stop_search_clean_silver.py       Cleaning + month derivation
│   │
│   └── gold/
│       └── enrich_crime_gold.py              Full gold pipeline (4 tables + validation)
│
├── .gitignore
└── README.md
```


The repository should contain **code, documentation and configuration only**. Raw Police.uk CSV/ZIP files should remain in Databricks, not GitHub.

---

## Pipeline Workflow

### Bronze (Raw Ingestion)

1. **police.uk data** — CSV archives from the police.uk data portal are uploaded to the UC volume and read via `spark.read.csv()`. Lineage columns (`_source_file`, `_archive_version`, `_ingested_at`) are appended.
2. **ONS population** — The `sapelsoasyoa20222024.xlsx` file is parsed using Python stdlib (`zipfile` + `xml.etree.ElementTree`) since `openpyxl` is unavailable on serverless compute. The file has 3 data sheets (Mid-2022/2023/2024), each with 35,672 LSOAs. Key columns (LAD code/name, LSOA code/name, total population) are extracted.
3. **IoD 2019 deprivation** — File 7 CSV is read with `spark.read.csv()`. All 57 columns (IMD scores, ranks, deciles + 7 domain scores + sub-domain scores + population denominators) are ingested with standardised snake_case names.

### Silver (Cleaning)

* Type casting (strings to double/long/int)
* Deduplication (crime_id tables via split-by-null; stop_search via dropDuplicates)
* Geographic validation (UK bounding box: 49-61N, -9 to 3E)
* Month derivation (string "yyyy-MM" to date; timestamp to date_trunc)
* force_id extraction from source filename

### Gold (Aggregation + Enrichment)

* 3 summary tables aggregated to business-ready grain with LSOA centroid coordinates
* Month-partitioned with dynamic partition overwrite (automation-ready)
* force_enrichment table joins ONS population + IoD deprivation to force areas via LSOA mapping
* Population-weighted deprivation scores for accurate force-level comparisons
* Full validation: row counts, measure totals, coordinate coverage, null key checks

---

## Key Design Decisions

* **Geographic grain**: street_crime/outcomes at LSOA level; stop_search at ~1.1km coordinate grid (no LSOA in source data)
* **Month partitioning**: all summary tables partitioned by month for incremental updates — only reprocesses months with new data
* **force_enrichment is static**: not month-partitioned since population/deprivation data updates annually, not monthly
* **XLSX parsing via stdlib**: `openpyxl`/`odfpy` unavailable on serverless compute — custom parser using `zipfile` + `ElementTree`
* **LSOA code vintage**: crime data uses 2021 LSOA codes; IoD 2019 uses 2011 codes — 89% match rate, unmatched LSOAs excluded from deprivation averages
* **Spark Connect (serverless)**: no RDD/SparkContext APIs; uses `withColumns({})` to avoid SCPAP004 lints

---

## Automation

### Monthly police.uk refresh

The gold pipeline uses `.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("month")` — re-running the pipeline with new monthly data only overwrites the affected month partitions. This makes it idempotent and automation-ready for scheduled jobs.

### Annual ONS population refresh

ONS publishes mid-year estimates annually (~June/July). When new data is released:
1. Download the new XLSX from the ONS website
2. Upload to `/Volumes/workspace/uk_crime_bronze/raw_data_files/exploratory/`
3. Re-run `bronze_population.py` (`.mode("overwrite")` — full refresh)
4. Re-create silver population table
5. Re-run gold pipeline to refresh `force_enrichment`

### ONS download

ONS download URLs return 404 from serverless compute. Files must be downloaded manually from:
`https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates`

---

## Current Status

| Layer | Status | Tables |
| --- | --- | --- |
| Bronze | ✅ Complete | street_crime, outcomes, stop_search, population_lsoa, deprivation_lsoa |
| Silver | ✅ Complete | street_crime, outcomes, stop_search, population_lsoa, deprivation_lsoa |
| Gold | ✅ Complete | street_crime_summary, outcomes_summary, stop_search_summary, force_enrichment |
| Dashboard | 🔲 Next | AI/BI dashboard with maps + enrichment |

### Data Coverage

* **Date range**: August 2023 – July 2026 (36 months)
* **Forces**: 44 police forces across England and Wales
* **LSOA match**: 100% population, 89% deprivation (2011 vs 2021 boundary changes)
* **Coordinate coverage**: 98.7–99.8% across all summary tables
* **Population**: 60.3M (ONS 2024 estimates for England + Wales)
