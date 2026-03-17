# iceberg_setup.py

import os
import shutil
import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.partitioning import PartitionSpec

# --------------------------------------------------
# CONFIG — Must match your Rustino loader
# --------------------------------------------------
WAREHOUSE = "file:///tmp/warehouse"
NAMESPACE = "default"
TABLE_NAME = "example_table"
TABLE_PATH = "/tmp/warehouse/default/example_table"
CATALOG_DB = "/tmp/iceberg_catalog.db"

# --------------------------------------------------
# CLEAN START (optional)
# --------------------------------------------------
if os.path.exists("/tmp/warehouse"):
    shutil.rmtree("/tmp/warehouse")

if os.path.exists(CATALOG_DB):
    os.remove(CATALOG_DB)

# --------------------------------------------------
# LOAD CATALOG (SQLite local catalog)
# --------------------------------------------------
catalog = load_catalog(
    "local",
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE,
)

# --------------------------------------------------
# CREATE NAMESPACE
# --------------------------------------------------
try:
    catalog.create_namespace(NAMESPACE)
except Exception:
    pass

# --------------------------------------------------
# DEFINE TABLE SCHEMA
# --------------------------------------------------
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
)

# --------------------------------------------------
# CREATE ICEBERG TABLE
# --------------------------------------------------
table = catalog.create_table(
    identifier=f"{NAMESPACE}.{TABLE_NAME}",
    schema=schema,
    partition_spec=PartitionSpec(),
)

# --------------------------------------------------
# SAMPLE DATA (matching schema exactly)
# --------------------------------------------------
df = pd.DataFrame({
    "id": [1, 2, 3],                     # required int
    "name": ["Alice", "Bob", "Charlie"], # optional string
})

# Ensure types match Iceberg schema
df["id"] = df["id"].astype("int32")

# Define PyArrow schema to enforce nullability and type
arrow_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
])

# Convert Pandas DataFrame to Arrow Table
arrow_table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)

# --------------------------------------------------
# APPEND DATA TO TABLE
# --------------------------------------------------
table.append(arrow_table)

# --------------------------------------------------
# VERIFY STRUCTURE
# --------------------------------------------------
print("\n✅ Iceberg table created at:", TABLE_PATH)
print("\n📁 Directory structure:")
for root, dirs, files in os.walk(TABLE_PATH):
    level = root.replace(TABLE_PATH, "").count(os.sep)
    indent = " " * 2 * level
    print(f"{indent}{os.path.basename(root)}/")
    subindent = " " * 2 * (level + 1)
    for f in files:
        print(f"{subindent}{f}")

# Check metadata presence
metadata_path = os.path.join(TABLE_PATH, "metadata")
if os.path.exists(metadata_path):
    files = os.listdir(metadata_path)
    if any(f.endswith(".metadata.json") for f in files):
        print("\n✅ VALID ICEBERG TABLE (metadata found)")
    else:
        print("\n❌ Metadata directory exists but no metadata.json found")
else:
    print("\n❌ Metadata directory missing")
