"""Exchange trade sources.

Two interchangeable sources return the same shape: a list of executed trades
(fills) for one UTC day.

- KrakenSource pulls real BTC/USD trades from Kraken's public REST API.
- SyntheticSource generates realistic trades offline, so the pipeline and CI
  run without network access.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import requests


@dataclass(frozen=True)
class ExchangeTrade:
    trade_id: str
    pair: str
    side: str  # "buy" | "sell"
    price_usd: Decimal
    quantity_btc: Decimal
    executed_at: datetime


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


class KrakenSource:
    """Public trades from Kraken (no API key needed).

    Kraken's `since` cursor is a nanosecond timestamp. We page forward from the
    start of the day and stop at the end of the day or at `max_trades`.
    """

    URL = "https://api.kraken.com/0/public/Trades"

    def __init__(self, pair: str = "XBTUSD", max_trades: int = 300, timeout: int = 20):
        self.pair = pair
        self.max_trades = max_trades
        self.timeout = timeout

    def fetch(self, day: date) -> list[ExchangeTrade]:
        start, end = _day_bounds(day)
        since = int(start.timestamp() * 1e9)
        trades: list[ExchangeTrade] = []

        while len(trades) < self.max_trades:
            resp = requests.get(
                self.URL, params={"pair": self.pair, "since": since}, timeout=self.timeout
            )
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("error"):
                raise RuntimeError(f"Kraken API error: {payload['error']}")

            result = payload["result"]
            cursor = int(result.pop("last"))
            rows = next(iter(result.values()), [])
            if not rows:
                break

            for row in rows:
                price, volume, ts, side = row[0], row[1], float(row[2]), row[3]
                executed_at = datetime.fromtimestamp(ts, tz=timezone.utc)
                if executed_at >= end:
                    return trades
                trade_id = str(row[6]) if len(row) > 6 else f"{ts:.4f}-{price}-{volume}"
                trades.append(
                    ExchangeTrade(
                        trade_id=f"KRK-{trade_id}",
                        pair="BTC/USD",
                        side="buy" if side == "b" else "sell",
                        price_usd=Decimal(price),
                        quantity_btc=Decimal(volume),
                        executed_at=executed_at,
                    )
                )
                if len(trades) >= self.max_trades:
                    return trades

            if cursor == since:
                break
            since = cursor
            time.sleep(1)  # stay well under Kraken's public rate limit

        return trades


class SyntheticSource:
    """Deterministic fake trades: same day in, same trades out."""

    def __init__(self, n_trades: int = 300, base_price: float = 95_000.0):
        self.n_trades = n_trades
        self.base_price = base_price

    def fetch(self, day: date) -> list[ExchangeTrade]:
        rng = random.Random(f"exchange-{day.isoformat()}")
        start, _ = _day_bounds(day)
        price = self.base_price * (1 + rng.uniform(-0.03, 0.03))
        offsets = sorted(rng.uniform(0, 86_399) for _ in range(self.n_trades))

        trades = []
        for i, offset in enumerate(offsets):
            price *= 1 + rng.gauss(0, 0.0008)
            trades.append(
                ExchangeTrade(
                    trade_id=f"SYN-{day:%Y%m%d}-{i:05d}",
                    pair="BTC/USD",
                    side=rng.choice(["buy", "sell"]),
                    price_usd=Decimal(f"{price:.1f}"),
                    quantity_btc=Decimal(f"{rng.lognormvariate(-4.5, 1.0):.8f}"),
                    executed_at=start + timedelta(seconds=offset),
                )
            )
        return trades
