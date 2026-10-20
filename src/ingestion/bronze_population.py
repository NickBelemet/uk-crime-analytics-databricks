import zipfile
import xml.etree.ElementTree as ET
import re
import os
from pyspark.sql.functions import current_timestamp, lit, col, F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import Row

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

# ONS LSOA mid-year population estimates (sapelsoasyoa20222024.xlsx)
# Covers mid-2022, mid-2023, mid-2024 — 3 sheets, 35,672 LSOAs each
#
# Download manually from the ONS website (download URLs return 404 from
# serverless compute, so manual download + volume upload is required):
#   https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/
#   populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates
#
# Save the XLSX to the exploratory volume directory before running:
XLSX_PATH = (
    "/Volumes/workspace/uk_crime_bronze/raw_data_files/exploratory/"
    "sapelsoasyoa20222024.xlsx"
)

TARGET_TABLE = "workspace.uk_crime_bronze.population_lsoa"

# --------------------------------------------------
# XLSX PARSER (stdlib only — openpyxl unavailable on serverless)
# --------------------------------------------------
#
# ONS file structure: 3 data sheets (rId5/6/7 = Mid-2022/2023/2024 LSOA 2021)
# Each sheet: 187 columns (LAD code/name, LSOA code/name, Total, F0-F90, M0-M90)
# Header at row 4, data starts row 5. 35,672 rows per sheet.
# We extract only columns A-E (indices 0-4) for bronze: LAD code/name, LSOA
# code/name, Total population. Full age/sex breakdowns (F0-F90, M0-M90) are
# available in the XLSX if needed later.

NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"

# ONS sheets: rId -> (year, sheet_name)
DATA_SHEETS = [
    ("rId5", 2022, "Mid-2022 LSOA 2021"),
    ("rId6", 2023, "Mid-2023 LSOA 2021"),
    ("rId7", 2024, "Mid-2024 LSOA 2021"),
]

# Only extract columns 0-4 (A-E): LAD code, LAD name, LSOA code, LSOA name, Total
TARGET_COL_INDICES = {0, 1, 2, 3, 4}
HEADER_ROW = 4  # Header at row 4, data starts at row 5


def _col_letter_to_index(col_str):
    """Convert column letters (A, B, AA) to 0-based index."""
    idx = 0
    for c in col_str:
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


# --------------------------------------------------
# 1. READ & PARSE XLSX
# --------------------------------------------------

if not os.path.exists(XLSX_PATH):
    raise FileNotFoundError(
        f"ONS population XLSX not found at:\n  {XLSX_PATH}\n\n"
        f"Download manually from the ONS website:\n"
        f"  https://www.ons.gov.uk/peoplepopulationandcommunity/"
        f"populationandmigration/populationestimates/datasets/"
        f"lowersuperoutputareamidyearpopulationestimates\n\n"
        f"Save the file to the exploratory volume directory:\n"
        f"  /Volumes/workspace/uk_crime_bronze/raw_data_files/exploratory/"
    )

print(f"Parsing XLSX: {XLSX_PATH}")
print(f"File size: {os.path.getsize(XLSX_PATH):,} bytes")

# --- Parse shared strings ---
print("\nParsing shared strings...")
with zipfile.ZipFile(XLSX_PATH, "r") as z:
    tree = ET.parse(z.open("xl/sharedStrings.xml"))
    root = tree.getroot()
    shared_strings = []
    for si in root.findall("main:si", NS):
        t = si.find("main:t", NS)
        if t is not None:
            shared_strings.append(t.text or "")
        else:
            texts = []
            for r in si.findall("main:r", NS):
                rt = r.find("main:t", NS)
                if rt is not None:
                    texts.append(rt.text or "")
            shared_strings.append("".join(texts))
    print(f"  Shared strings: {len(shared_strings):,}")

    # Read workbook.xml.rels for rId -> file mapping
    tree = ET.parse(z.open("xl/_rels/workbook.xml.rels"))
    root = tree.getroot()
    rels = {}
    for rel in root:
        rels[rel.get("Id")] = rel.get("Target")


# --------------------------------------------------
# 2. EXTRACT DATA FROM ALL THREE SHEETS
# --------------------------------------------------

all_data = []

for rId, year, sheet_name in DATA_SHEETS:
    print(f"\nParsing sheet: {sheet_name} (year={year})...")
    target = "xl/" + rels[rId]

    with zipfile.ZipFile(XLSX_PATH, "r") as z:
        tree = ET.parse(z.open(target))
        root = tree.getroot()
        sheet_data = root.find("main:sheetData", NS)

        row_count = 0
        for row_elem in sheet_data.findall("main:row", NS):
            row_num = int(row_elem.get("r", "0"))
            if row_num < HEADER_ROW + 1:
                continue

            row_dict = {}
            for cell_elem in row_elem.findall("main:c", NS):
                ref = cell_elem.get("r", "")
                col_match = re.match(r"([A-Z]+)", ref)
                if not col_match:
                    continue
                col_idx = _col_letter_to_index(col_match.group(1))
                if col_idx not in TARGET_COL_INDICES:
                    continue

                cell_type = cell_elem.get("t", "")
                v_elem = cell_elem.find("main:v", NS)
                if v_elem is None:
                    row_dict[col_idx] = ""
                elif cell_type == "s":
                    idx = int(v_elem.text)
                    row_dict[col_idx] = (
                        shared_strings[idx]
                        if idx < len(shared_strings)
                        else ""
                    )
                else:
                    row_dict[col_idx] = v_elem.text

            lad_code = row_dict.get(0, "")
            lad_name = row_dict.get(1, "")
            lsoa_code = row_dict.get(2, "")
            lsoa_name = row_dict.get(3, "")
            total_pop = row_dict.get(4, "")

            if not lsoa_code and not total_pop:
                continue

            try:
                total_pop_int = int(float(total_pop)) if total_pop else None
            except (ValueError, TypeError):
                total_pop_int = None

            all_data.append(Row(
                lad_code=str(lad_code).strip() if lad_code else None,
                lad_name=str(lad_name).strip() if lad_name else None,
                lsoa_code=str(lsoa_code).strip() if lsoa_code else None,
                lsoa_name=str(lsoa_name).strip() if lsoa_name else None,
                total_population=total_pop_int,
                year=year,
            ))
            row_count += 1

        print(f"  Extracted {row_count:,} rows")

print(f"\nTotal rows across all sheets: {len(all_data):,}")


# --------------------------------------------------
# 3. CREATE SPARK DATAFRAME
# --------------------------------------------------

schema = StructType([
    StructField("lad_code", StringType(), True),
    StructField("lad_name", StringType(), True),
    StructField("lsoa_code", StringType(), True),
    StructField("lsoa_name", StringType(), True),
    StructField("total_population", LongType(), True),
    StructField("year", LongType(), True),
])

bronze_df = spark.createDataFrame(all_data, schema=schema)

print("\n=== DataFrame Schema ===")
bronze_df.printSchema()
print(f"Row count: {bronze_df.count():,}")

display(bronze_df.limit(10))


# --------------------------------------------------
# 4. ADD BRONZE LINEAGE METADATA
# --------------------------------------------------

bronze_df = (
    bronze_df
    .withColumn("_source_file", lit(XLSX_PATH))
    .withColumn("_source_dataset", lit("ONS LSOA mid-year population estimates"))
    .withColumn("_archive_version", lit("mid-2022-2024"))
    .withColumn("_ingested_at", current_timestamp())
)


# --------------------------------------------------
# 5. VALIDATE
# --------------------------------------------------

record_count = bronze_df.count()
print(f"\nTotal population records: {record_count:,}")

# Check LSOA code coverage
non_null_lsoa = bronze_df.filter(col("lsoa_code").isNotNull()).count()
print(f"Records with LSOA code: {non_null_lsoa:,} ({100*non_null_lsoa/record_count:.1f}%)")

# Show LSOA code prefixes
print("\nLSOA code prefixes:")
bronze_df.select(
    F.substring("lsoa_code", 1, 1).alias("prefix")
).groupBy("prefix").count().orderBy("prefix").show()

# Population totals by year
print("Population totals by year:")
bronze_df.groupBy("year") \
    .agg(
        F.sum("total_population").alias("total_pop"),
        F.count("*").alias("row_count"),
    ) \
    .orderBy("year").show()


# --------------------------------------------------
# 6. WRITE BRONZE DELTA TABLE
# --------------------------------------------------

print(f"\nWriting Bronze Delta table: {TARGET_TABLE}")

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(f"Bronze table successfully created: {TARGET_TABLE}")
print(f"Rows written: {record_count:,}")
