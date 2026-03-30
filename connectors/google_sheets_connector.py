

import pandas as pd
import polars as pl
import requests
from io import StringIO
import re


def convert_to_export_url(sheet_url: str, gid: str = "0") -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError("Invalid Google Sheets URL")
    sheet_id = match.group(1)
    gid_match = re.search(r"gid=(\d+)", sheet_url)
    if gid_match:
        gid = gid_match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def clean_columns(df, engine: str = "pandas"):
    """
    Fix common column issues after reading Google Sheets:
    - Empty column names
    - Duplicate column names
    - Special characters / spaces
    - Leading numbers
    """
    if engine == "pandas":
        cols = list(df.columns)
    else:
        cols = list(df.columns)

    cleaned = []
    seen = {}

    for i, col in enumerate(cols):
        # 1. Strip whitespace
        col = str(col).strip()

        # 2. Replace empty name with 'col_N'
        if col == "" or col == "nan" or col == "None":
            col = f"col_{i}"

        # 3. Replace spaces and special characters with underscore
        col = re.sub(r"[^a-zA-Z0-9_]", "_", col)

        # 4. If starts with a number, prefix with 'col_'
        if re.match(r"^\d", col):
            col = f"col_{col}"

        # 5. Lowercase (optional but good for SQL)
        col = col.lower()

        # 6. Handle duplicates → name, name_1, name_2 ...
        if col in seen:
            seen[col] += 1
            col = f"{col}_{seen[col]}"
        else:
            seen[col] = 0

        cleaned.append(col)

    # Apply cleaned column names
    if engine == "pandas":
        df.columns = cleaned
    else:
        df = df.rename(dict(zip(df.columns, cleaned)))

    return df


def find_header_row(response_text: str) -> int:
    """Find the first non-empty row to use as header"""
    lines = response_text.splitlines()
    for i, line in enumerate(lines):
        # A real header row has mostly non-empty values
        values = [v.strip() for v in line.split(",")]
        non_empty = [v for v in values if v != ""]
        if len(non_empty) >= 2:  # at least 2 columns have values
            return i
    return 0  # fallback


def google_sheet_connector(sheet_url: str, engine: str = "pandas"):
    export_url = convert_to_export_url(sheet_url)
    response = requests.get(export_url)

    content_type = response.headers.get("Content-Type", "")

    if response.status_code != 200:
        raise ConnectionError(f"Failed to fetch sheet. Status: {response.status_code}")

    if "text/html" in content_type:
        raise PermissionError(
            "Google returned HTML instead of CSV.\n"
            "Make sheet public: Share → Anyone with the link → Viewer"
        )

    # ✅ Auto-detect header row
    header_row = find_header_row(response.text)
    print(f"📍 Header detected at row index: {header_row}")

    if engine == "pandas":
        df = pd.read_csv(
            StringIO(response.text),
            header=header_row,         # ← dynamic header row
            skip_blank_lines=False,
        )
        df = df.dropna(axis=1, how="all")
        df = df.dropna(axis=0, how="all")
        df = clean_columns(df, engine="pandas")

    elif engine == "polars":
        # Polars: skip rows before header manually
        lines = response.text.splitlines()
        csv_from_header = "\n".join(lines[header_row:])
        df = pl.read_csv(
            StringIO(csv_from_header),
            infer_schema_length=10000,
            ignore_errors=True,
        )
        df = df[[col for col in df.columns if df[col].null_count() < len(df)]]
        df = clean_columns(df, engine="polars")

    else:
        raise ValueError(f"Unsupported engine '{engine}'. Use 'pandas' or 'polars'.")

    print(f"✅ Loaded {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"📋 Columns: {list(df.columns)}")
    return df



# sheet_url = "https://docs.google.com/spreadsheets/d/1syOiD7p1UMN63WRNn0vt4HEJIbxvavvQHZLFcF3KZrY/edit?usp=sharing"

# df = google_sheet_connector(sheet_url, engine="pandas")
# print(df.head())