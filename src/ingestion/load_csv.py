import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = ROOT / "dataset" / "HRDataset_v14.csv"

DB_USER = os.getenv("POSTGRES_USER", "hr_admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hr_password")
DB_NAME = os.getenv("POSTGRES_DB", "hr")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")


def load_csv_to_raw():
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    df.to_sql("hr_employees", engine, schema="raw", if_exists="replace", index=False)

    print(f"Loaded {len(df)} rows into raw.hr_employees")


if __name__ == "__main__":
    load_csv_to_raw()
