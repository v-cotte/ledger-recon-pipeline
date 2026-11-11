"""Simulate the two internal sources that get reconciled against the exchange.

For every exchange trade we produce:
- a ledger entry (what our platform booked, normally from the app's Postgres)
- a provider statement line (what the venue/custodian says settled)

A small, deterministic share of trades gets a planted break, and every planted
break is written to a ground-truth table. The dbt tests use that table to prove
the reconciliation model finds every break and invents none.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from ingestion.sources import ExchangeTrade

FEE_RATE = Decimal("0.0025")
WRONG_FEE_RATE = Decimal("0.0040")  # e.g. a fee tier that was never applied
CENTS = Decimal("0.01")

ISSUE_TYPES = (
    "missing_in_ledger",
    "duplicate_in_ledger",
    "amount_mismatch",
    "fee_mismatch",
    "missing_in_statement",
    "late_settlement",
)


@dataclass
class LedgerEntry:
    entry_id: str
    trade_ref: str
    account_id: str
    side: str
    quantity_btc: Decimal
    price_usd: Decimal
    notional_usd: Decimal
    fee_usd: Decimal
    booked_at: datetime


@dataclass
class StatementLine:
    statement_line_id: str
    trade_ref: str
    side: str
    settled_quantity_btc: Decimal
    settled_amount_usd: Decimal
    fee_usd: Decimal
    settlement_date: date


@dataclass
class PlantedIssue:
    trade_ref: str
    issue_type: str


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENTS, rounding=ROUND_HALF_UP)


def simulate(
    day: date, trades: list[ExchangeTrade], break_rate: float = 0.01
) -> tuple[list[LedgerEntry], list[StatementLine], list[PlantedIssue]]:
    rng = random.Random(f"internal-{day.isoformat()}")

    # Pick a disjoint set of trades for each issue type.
    per_type = max(1, round(len(trades) * break_rate))
    pool = rng.sample(range(len(trades)), k=min(len(trades), per_type * len(ISSUE_TYPES)))
    issue_by_index = {
        idx: ISSUE_TYPES[i // per_type] for i, idx in enumerate(pool) if i // per_type < len(ISSUE_TYPES)
    }

    accounts = [f"ACC-{n:04d}" for n in range(1, 51)]
    ledger: list[LedgerEntry] = []
    statement: list[StatementLine] = []
    issues: list[PlantedIssue] = []

    for i, t in enumerate(trades):
        issue = issue_by_index.get(i)
        if issue:
            issues.append(PlantedIssue(trade_ref=t.trade_id, issue_type=issue))

        notional = _money(t.price_usd * t.quantity_btc)
        fee = _money(notional * FEE_RATE)

        # --- internal ledger -------------------------------------------------
        if issue != "missing_in_ledger":
            ledger_price, ledger_notional, ledger_fee = t.price_usd, notional, fee
            if issue == "amount_mismatch":
                # a fat-fingered price on booking
                ledger_price = t.price_usd * Decimal(str(1 + rng.choice([-1, 1]) * rng.uniform(0.01, 0.05)))
                ledger_price = ledger_price.quantize(Decimal("0.1"))
                ledger_notional = _money(ledger_price * t.quantity_btc)
            if issue == "fee_mismatch":
                ledger_fee = _money(notional * WRONG_FEE_RATE)

            entry = LedgerEntry(
                entry_id=f"LED-{t.trade_id}",
                trade_ref=t.trade_id,
                account_id=rng.choice(accounts),
                side=t.side,
                quantity_btc=t.quantity_btc,
                price_usd=ledger_price,
                notional_usd=ledger_notional,
                fee_usd=ledger_fee,
                booked_at=t.executed_at + timedelta(seconds=rng.randint(1, 30)),
            )
            ledger.append(entry)
            if issue == "duplicate_in_ledger":
                ledger.append(
                    LedgerEntry(**{**entry.__dict__, "entry_id": f"{entry.entry_id}-DUP",
                                   "booked_at": entry.booked_at + timedelta(minutes=5)})
                )

        # --- provider statement ----------------------------------------------
        if issue != "missing_in_statement":
            # Providers round differently: up to 1 cent of noise must NOT break.
            noise = Decimal(rng.choice(["0.00", "0.00", "0.01", "-0.01"]))
            settlement = day + timedelta(days=2 if issue == "late_settlement" else 0)
            statement.append(
                StatementLine(
                    statement_line_id=f"STM-{t.trade_id}",
                    trade_ref=t.trade_id,
                    side=t.side,
                    settled_quantity_btc=t.quantity_btc,
                    settled_amount_usd=notional + noise,
                    fee_usd=fee,
                    settlement_date=settlement,
                )
            )

    return ledger, statement, issues
