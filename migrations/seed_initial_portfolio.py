"""Safely preview or apply the initial holdings and cash migration."""

import argparse
import sys
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main import KTRADE_BASELINE_HOLDINGS, KTRADE_LOCKED_CASH  # noqa: E402
from portfolio_manager import REAL_KTRADE_WORKING_CAPITAL, get_db_connection  # noqa: E402


def _migration_rows():
    rows = []
    for ticker, holding in KTRADE_BASELINE_HOLDINGS.items():
        shares = Decimal(str(holding["shares"]))
        invested_cost = Decimal(str(holding["invested_cost"]))
        rows.append({
            "ticker": ticker.upper(),
            "shares": shares,
            "avg_buy_price": (invested_cost / shares).quantize(Decimal("0.0001")),
            "invested_cost": invested_cost,
            "avg_display": Decimal(str(holding["avg_display"])),
        })
    return rows


def _read_conflicts(cur, rows):
    cur.execute("SELECT UPPER(ticker), shares, invested_cost, avg_buy_price FROM portfolio_holdings;")
    existing_holdings = {
        row[0]: {
            "shares": Decimal(str(row[1])),
            "invested_cost": Decimal(str(row[2])) if row[2] is not None else None,
            "avg_buy_price": Decimal(str(row[3])),
        }
        for row in cur.fetchall()
    }

    missing = []
    conflicts = []
    for row in rows:
        existing = existing_holdings.get(row["ticker"])
        if existing is None:
            missing.append(row)
            continue
        existing_cost = existing["invested_cost"]
        if existing_cost is None:
            existing_cost = existing["shares"] * existing["avg_buy_price"]
        if existing["shares"] != row["shares"] or existing_cost.quantize(Decimal("0.01")) != row["invested_cost"].quantize(Decimal("0.01")):
            conflicts.append({
                "ticker": row["ticker"],
                "existing_shares": existing["shares"],
                "existing_cost": existing_cost,
                "expected_shares": row["shares"],
                "expected_cost": row["invested_cost"],
            })

    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(CASE
            WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
            WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
            WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
            ELSE 0
        END), 0)
        FROM capital_ledger;
    """)
    ledger_count, current_cash = cur.fetchone()
    current_cash = Decimal(str(current_cash))
    expected_cash = Decimal(str(KTRADE_LOCKED_CASH))
    if ledger_count and current_cash.quantize(Decimal("0.01")) != expected_cash.quantize(Decimal("0.01")):
        conflicts.append({
            "cash_ledger_rows": ledger_count,
            "existing_cash": current_cash,
            "expected_cash": expected_cash,
        })

    cash_insert = not ledger_count
    return missing, conflicts, cash_insert, current_cash


def run(apply=False):
    rows = _migration_rows()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        cur.execute("SET lock_timeout = '2000'; SET statement_timeout = '5000';")
        cur.execute("LOCK TABLE portfolio_holdings, capital_ledger IN SHARE ROW EXCLUSIVE MODE;")
        missing, conflicts, cash_insert, current_cash = _read_conflicts(cur, rows)

        print("Migration preview (quote prices are intentionally excluded):")
        for row in missing:
            print(
                f"  INSERT holding {row['ticker']}: shares={row['shares']}, "
                f"invested_cost={row['invested_cost']}, avg_buy_price={row['avg_buy_price']}, "
                f"last_price=NULL"
            )
        if cash_insert:
            working_capital = Decimal(str(REAL_KTRADE_WORKING_CAPITAL))
            adjustment = Decimal(str(KTRADE_LOCKED_CASH)) - working_capital
            print(
                f"  INSERT cash deposit: amount={working_capital} PKR; "
                f"INSERT deployed adjustment: amount={adjustment} PKR"
            )
        else:
            print(f"  Cash ledger already has rows; current computed cash={current_cash} PKR (no cash insert).")
        if not missing and not cash_insert:
            print("  No inserts required.")
        if conflicts:
            print("Conflicts found; no changes will be applied:")
            for conflict in conflicts:
                print(f"  {conflict}")
            conn.rollback()
            return 2
        if not apply:
            print("DRY RUN ONLY: no database changes made.")
            conn.rollback()
            return 0

        for row in missing:
            cur.execute("""
                INSERT INTO portfolio_holdings (
                    ticker, shares, avg_buy_price, invested_cost, avg_display,
                    last_price, price_updated_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NULL, NULL, NOW())
                ON CONFLICT (ticker) DO NOTHING;
            """, (
                row["ticker"],
                row["shares"],
                row["avg_buy_price"],
                row["invested_cost"],
                row["avg_display"],
            ))
        if cash_insert:
            working_capital = Decimal(str(REAL_KTRADE_WORKING_CAPITAL))
            adjustment = Decimal(str(KTRADE_LOCKED_CASH)) - working_capital
            cur.execute("""
                INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
                VALUES ('DEPOSIT_TO_KTRADE', %s, %s, %s, NOW(), NOW());
            """, (working_capital, "Initial portfolio migration", "Portfolio migration"))
            cur.execute("""
                INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
                VALUES ('FEE_ADJUSTMENT', %s, %s, %s, NOW(), NOW());
            """, (adjustment, "Initial deployed capital migration", "Portfolio migration"))
        conn.commit()
        print("APPLIED: migration inserts committed.")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply inserts after showing the migration preview")
    args = parser.parse_args()
    raise SystemExit(run(apply=args.apply))
