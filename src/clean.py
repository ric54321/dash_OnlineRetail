import argparse
from pathlib import Path
import pandas as pd


# -----------------------------
# Core ETL functions (CLEAN ONLY)
# -----------------------------
def load_raw(path: Path) -> pd.DataFrame:
    """Load raw CSV (from Kaggle)."""
    df = pd.read_csv(path, encoding="latin-1")
    print(f"[load] shape={df.shape} from {path}")
    return df


def basic_clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to snake_case, fix dtypes, drop unusable rows, normalize text, add derived fields."""
    df = raw.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    }).copy()

    # dtypes
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # remove unusable rows for analysis
    df = df.dropna(subset=["invoice_date", "stock_code", "description"]).copy()

    # text hygiene
    df["description"] = df["description"].str.strip()
    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip()

    # derived
    df["is_credit_note"] = df["invoice_no"].astype(str).str.startswith("C")
    df["line_total"] = df["quantity"] * df["unit_price"]
    df["invoice_date_date"] = df["invoice_date"].dt.date
    df["invoice_ym"] = df["invoice_date"].dt.to_period("M").astype(str)

    print(f"[clean] shape={df.shape}")
    return df


def split_sales_returns(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into sales (valid) and returns (credit/negatives/invalid lines)."""
    sales = df[~df["is_credit_note"] & (df["quantity"] > 0) & (df["unit_price"] > 0)].copy()
    returns = df[df["is_credit_note"] | (df["quantity"] <= 0) | (df["unit_price"] <= 0)].copy()
    print(f"[split] sales={sales.shape} returns={returns.shape}")
    return sales, returns


def build_dimensions(sales: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Optional dimensional tables and fact with known customer only."""
    sales_with_cust = sales.dropna(subset=["customer_id"]).copy()
    dim_products = sales[["stock_code", "description"]].drop_duplicates().reset_index(drop=True)
    dim_customers = sales_with_cust[["customer_id", "country"]].drop_duplicates().reset_index(drop=True)
    dim_invoices = (
        sales[["invoice_no", "invoice_date"]]
        .drop_duplicates()
        .sort_values("invoice_date")
        .reset_index(drop=True)
    )
    return {
        "fact_sales_lines_with_customer": sales_with_cust,
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "dim_invoices": dim_invoices,
    }


def save_outputs(
    out_dir: Path,
    sales: pd.DataFrame,
    returns: pd.DataFrame,
    extras: dict[str, pd.DataFrame] | None = None,
    write_parquet: bool = True,  # default ON per your request
) -> None:
    """Write cleaned outputs to CSV (+ Parquet for sales if enabled)."""
    out_dir.mkdir(parents=True, exist_ok=True)

    sales_csv = out_dir / "fact_sales_lines.csv"
    returns_csv = out_dir / "fact_returns_lines.csv"
    sales.to_csv(sales_csv, index=False)
    returns.to_csv(returns_csv, index=False)
    print(f"[save] {sales_csv}")
    print(f"[save] {returns_csv}")

    if write_parquet:
        sales_parq = out_dir / "fact_sales_lines.parquet"
        try:
            sales.to_parquet(sales_parq, index=False)
            print(f"[save] {sales_parq}")
        except Exception as e:
            print(f"[warn] parquet not written: {e}")

    if extras:
        for name, dfx in extras.items():
            p = out_dir / f"{name}.csv"
            dfx.to_csv(p, index=False)
            print(f"[save] {p}")


# -----------------------------
# CLI
# -----------------------------
def parse_args():
    project_root = Path(__file__).resolve().parents[1]
    default_raw = project_root / "data" / "raw" / "OnlineRetail.csv" 
    default_out = project_root / "data" / "clean"

    p = argparse.ArgumentParser(description="Online Retail cleaner (CSV → clean marts)")
    p.add_argument("--raw", type=Path, default=default_raw, help="Path to raw CSV (default: data/raw/OnlineRetail.csv)")
    p.add_argument("--out", type=Path, default=default_out, help="Output folder (default: data/clean)")
    p.add_argument("--parquet", type=int, default=1, help="Write Parquet for sales? 1=yes, 0=no (default 1)")
    p.add_argument("--dims", type=int, default=1, help="Generate dimensions and fact with known customers? 1=yes, 0=no (default 1)")
    return p.parse_args()


def main():
    args = parse_args()

    if not args.raw.exists():
        raise FileNotFoundError(
            f"Raw CSV not found at: {args.raw}\n"
            f"Put the file under data/raw or pass --raw to point to a different path."
        )

    raw = load_raw(args.raw)
    df = basic_clean(raw)
    sales, returns = split_sales_returns(df)

    extras = build_dimensions(sales) if args.dims else None
    save_outputs(args.out, sales, returns, extras=extras, write_parquet=bool(args.parquet))

    print("[done] cleaning completed.")


if __name__ == "__main__":
    main()
