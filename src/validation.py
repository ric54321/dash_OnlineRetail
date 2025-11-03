import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def edges_spaces(s: pd.Series) -> int:
    """Count values with leading or trailing spaces."""
    s = s.astype(str)
    return ((s.str[:1] == " ") | (s.str[-1:] == " ")).sum()


def run_validation(sales_path: Path, returns_path: Path) -> pd.DataFrame:
    """Validate cleaned outputs (sales/returns) without re-running the ETL."""
    if not sales_path.exists() or not returns_path.exists():
        raise FileNotFoundError(f"Missing files: {sales_path} or {returns_path}")

    sales = pd.read_csv(sales_path)
    returns = pd.read_csv(returns_path)
    df = pd.concat([
        sales.assign(_source="sales"),
        returns.assign(_source="returns")
    ], ignore_index=True)

    results = []

    def add(name, ok, detail=""):
        results.append({"check": name, "ok": bool(ok), "detail": detail})

    # Required columns
    required_cols = [
        "invoice_no","stock_code","description","quantity",
        "invoice_date","unit_price","customer_id","country",
        "is_credit_note","line_total"
    ]
    add("columns_present", set(required_cols).issubset(df.columns),
        detail=str(sorted(set(required_cols) - set(df.columns))))

    # Basic dtypes (best effort)
    try:
        pd.to_datetime(df["invoice_date"], errors="raise")
        q_ok = pd.to_numeric(df["quantity"], errors="coerce").notna().all()
        p_ok = pd.to_numeric(df["unit_price"], errors="coerce").notna().all()
        add("dtypes_basic(invoice_date/quantity/unit_price)", q_ok and p_ok)
    except Exception as e:
        add("dtypes_basic(invoice_date/quantity/unit_price)", False, detail=str(e))

    # Trims (string cleaning)
    add("trim_description", edges_spaces(df["description"]) == 0)
    add("trim_stock_code", edges_spaces(df["stock_code"]) == 0)
    add("trim_country", edges_spaces(df["country"]) == 0)

    # line_total consistency
    qt = pd.to_numeric(df["quantity"], errors="coerce")
    up = pd.to_numeric(df["unit_price"], errors="coerce")
    lt = pd.to_numeric(df["line_total"], errors="coerce")
    close = np.isclose(lt, qt * up, rtol=1e-6, atol=1e-9)
    add("line_total_consistency", close.all(), detail=f"mismatches={(~close).sum()}")

    # sales checks
    s_q_ok = (pd.to_numeric(sales["quantity"], errors="coerce") > 0).all()
    s_p_ok = (pd.to_numeric(sales["unit_price"], errors="coerce") > 0).all()
    s_c_ok = (~sales["is_credit_note"].astype(bool)).all()
    add("quantity_positive_in_sales", s_q_ok)
    add("unit_price_positive_in_sales", s_p_ok)
    add("no_credit_notes_in_sales", s_c_ok)

    # returns non-empty (expected for this dataset)
    add("returns_nonempty_expected", len(returns) > 0, detail=f"returns={len(returns)}")

    rep = pd.DataFrame(results)
    print("=== VALIDATION REPORT ===")
    print(rep.to_string(index=False))
    return rep


def parse_args():
    project_root = Path(__file__).resolve().parents[1]
    default_sales = project_root / "data" / "clean" / "fact_sales_lines.csv"
    default_returns = project_root / "data" / "clean" / "fact_returns_lines.csv"

    p = argparse.ArgumentParser(description="Validate cleaned Online Retail outputs")
    p.add_argument("--sales", type=Path, default=default_sales, help="Path to fact_sales_lines.csv")
    p.add_argument("--returns", type=Path, default=default_returns, help="Path to fact_returns_lines.csv")
    return p.parse_args()


def main():
    args = parse_args()
    run_validation(args.sales, args.returns)
    print("[done] validation completed.")


if __name__ == "__main__":
    main()
