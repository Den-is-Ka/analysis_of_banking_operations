from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from src.utils import (
    filter_month_to_date,
    format_date,
    get_currency_rates,
    get_stock_prices,
    human_greeting,
    last4,
    load_transactions_xlsx,
    parse_dt,
    read_user_settings,
)

logger = logging.getLogger(__name__)


def _build_home_payload(df: pd.DataFrame, at: datetime, settings: dict) -> dict[str, Any]:
    # Приветствие
    greeting = human_greeting(at)

    df_cards = df.copy()
    # последние 4 цифры
    df_cards["last_digits"] = df_cards["card"].apply(last4).fillna("")
    # считаем траты как сумму модулей отрицательных amount
    df_cards["spent"] = df_cards["amount"].apply(lambda x: -x if x < 0 else 0.0)

    cards: list[dict[str, Any]] = []
    grouped_cards = (
        df_cards.groupby("last_digits", dropna=False)["spent"]
        .sum()
        .reset_index()
    )
    for _, row in grouped_cards.iterrows():
        total_spent = float(row["spent"])
        cards.append(
            {
                "last_digits": str(row["last_digits"]),
                "total_spent": round(total_spent, 2),
                "cashback": round(total_spent * 0.01, 2),
            }
        )
    cards.sort(key=lambda x: x["last_digits"])

    # Топ транзакций
    month_df = filter_month_to_date(df, at)
    if month_df.empty:
        top_transactions: list[dict[str, Any]] = []
    else:
        top = (
            month_df.assign(abs_amount=month_df["amount"].abs())
            .sort_values("abs_amount", ascending=False)
            .head(5)
            .drop(columns=["abs_amount"])
        )
        top_transactions = [
            {
                "date": format_date(r["date"]),
                "amount": float(r["amount"]),
                "category": str(r["category"]),
                "description": str(r["description"]),
            }
            for r in top.to_dict(orient="records")
        ]

    currency_codes = settings.get("user_currencies", [])
    stock_codes = settings.get("user_stocks", [])
    currency_rates = get_currency_rates(currency_codes)
    stock_prices = get_stock_prices(stock_codes)

    return {
        "greeting": greeting,
        "cards": cards,
        "top_transactions": top_transactions,
        "currency_rates": currency_rates,
        "stock_prices": stock_prices,
    }


def home_view(dt_str: str, path: str = "data/operations.xlsx") -> str:
    """Главная страница: принимает строку даты 'YYYY-MM-DD HH:MM:SS' и возвращает JSON-строку."""
    at = parse_dt(dt_str)
    settings = read_user_settings("user_settings.json")
    df = load_transactions_xlsx(path)
    payload = _build_home_payload(df, at, settings)
    return json.dumps(payload, indent=4, ensure_ascii=False)


def home_view_with_df(df: pd.DataFrame, dt_str: str, settings_path: str = "user_settings.json") -> str:
    at = parse_dt(dt_str)
    settings = read_user_settings(settings_path)
    payload = _build_home_payload(df, at, settings)
    return json.dumps(payload,indent=4, ensure_ascii=False)
