import psycopg2
import re
import polars as pl
import pandas as pd
import numpy as np
# ─────────────────────────────────────────────
# CLEAN COLUMN NAMES
# ─────────────────────────────────────────────

def clean_column(name):
    name = str(name).lower().strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"^_+|_+$", "", name)
    if re.match(r"^\d", name):
        name = f"col_{name}"
    return name or "unnamed"


# ─────────────────────────────────────────────
# GET SCHEMA
# ─────────────────────────────────────────────

def get_schema(df) -> dict:
    if isinstance(df, pl.DataFrame):
        return {col: str(dtype) for col, dtype in df.schema.items()}
    elif isinstance(df, pd.DataFrame):
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df)}")


# ─────────────────────────────────────────────
# DTYPE → PostgreSQL TYPE MAPPING
# ─────────────────────────────────────────────

def dtype_to_sql(dtype: str) -> str:
    dtype = dtype.lower()
    if "int" in dtype:
        return "BIGINT"
    elif "float" in dtype or "double" in dtype:
        return "DOUBLE PRECISION"
    elif "bool" in dtype:
        return "BOOLEAN"
    elif "datetime" in dtype or "timestamp" in dtype:
        return "TIMESTAMP"
    elif "date" in dtype:
        return "DATE"
    else:
        return "TEXT"


# ─────────────────────────────────────────────
# GET ALL TABLES
# ─────────────────────────────────────────────

def get_all_tables(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    return [t[0] for t in cursor.fetchall()]


# ─────────────────────────────────────────────
# GET TABLE COLUMNS
# ─────────────────────────────────────────────

def get_table_columns(cursor, table_name):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return [r[0] for r in cursor.fetchall()]


# ─────────────────────────────────────────────
# TABLE EXISTS CHECK
# ─────────────────────────────────────────────

def table_exists(cursor, table_name):
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = %s
        );
    """, (table_name,))
    return cursor.fetchone()[0]


# ─────────────────────────────────────────────
# CREATE TABLE
# ─────────────────────────────────────────────

def create_table(cursor, df, table_name):
    schema = get_schema(df)

    col_definitions = []
    for col, dtype in schema.items():
        sql_type   = dtype_to_sql(dtype)
        quoted_col = f'"{col}"'
        col_definitions.append(f'{quoted_col} {sql_type}')

    col_sql       = ',\n        '.join(col_definitions)
    quoted_table  = f'"{table_name}"'

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {quoted_table} (
        {col_sql}
    );
    """
    cursor.execute(create_query)
    print(f"✅ Table '{table_name}' created successfully")


# ─────────────────────────────────────────────
# INSERT DATA
# ─────────────────────────────────────────────

def insert_data(cursor, df, table_name):
    if isinstance(df, pl.DataFrame):
        pdf = df.to_pandas()
    else:
        pdf = df

    quoted_cols   = [f'"{c}"' for c in pdf.columns]
    cols          = ', '.join(quoted_cols)
    placeholders  = ', '.join(['%s'] * len(pdf.columns))
    quoted_table  = f'"{table_name}"'

    insert_query = f'INSERT INTO {quoted_table} ({cols}) VALUES ({placeholders})'

    rows_inserted = 0
    for row in pdf.itertuples(index=False, name=None):
        clean_row = tuple(
            None if (val is None or (isinstance(val, float) and np.isnan(val)))
            else val.item() if isinstance(val, (np.integer, np.floating))
            else str(val) if isinstance(val, np.bool_)
            else val
            for val in row
        )
        cursor.execute(insert_query, clean_row)
        rows_inserted += 1

    print(f"✅ {rows_inserted} rows inserted into '{table_name}'")


# ─────────────────────────────────────────────
# MAIN LOAD FUNCTION
# ─────────────────────────────────────────────

def load_to_db(df, option=None, table_name=None):
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="spark@1234",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # ✅ Clean column names
    if isinstance(df, pl.DataFrame):
        df = df.rename({col: clean_column(col) for col in df.columns})
    else:
        df.columns = [clean_column(c) for c in df.columns]

    print(f"📋 Columns: {list(df.columns)}")

    # ──────────────────────────────────────────
    # OPTION 1 → APPEND
    # add to existing table if exists, else create new
    # ──────────────────────────────────────────
    if option == "1":
        if not table_name:
            raise ValueError("table_name is required for option=1 ❌")

        if not table_exists(cursor, table_name):
            print(f"⚠️ Table '{table_name}' not found — creating automatically...")
            create_table(cursor, df, table_name)
        else:
            print(f"📥 Appending into existing table: '{table_name}'")

        insert_data(cursor, df, table_name)

    # ──────────────────────────────────────────
    # OPTION 2 → OVERWRITE
    # if table exist then drop + recreate, else create new
    # otherwise fresh create
    # ──────────────────────────────────────────
    elif option == "2":
        if not table_name:
            raise ValueError("table_name is required for option=2 ❌")

        if table_exists(cursor, table_name):
            print(f"⚠️ Table '{table_name}' exists — dropping and recreating...")
            cursor.execute(f'DROP TABLE "{table_name}"')

        print(f"🆕 Creating table: '{table_name}'")
        create_table(cursor, df, table_name)
        insert_data(cursor, df, table_name)

    # ──────────────────────────────────────────
    # OPTION 3 → CREATE ONLY (strict)
    # Table already exist provide error, otherwise create new
    # ──────────────────────────────────────────
    elif option == "3":
        if not table_name:
            raise ValueError("table_name is required for option=3 ❌")

        if table_exists(cursor, table_name):
            raise ValueError(f"Table '{table_name}' already exists ❌ Use option=2 to overwrite")

        print(f"🆕 Creating new table: '{table_name}'")
        create_table(cursor, df, table_name)
        insert_data(cursor, df, table_name)

    else:
        cursor.close()
        conn.close()
        raise ValueError(f"Invalid option: '{option}' ❌ Use 1 (append), 2 (overwrite), 3 (create new)")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n✅ Pipeline completed successfully!")