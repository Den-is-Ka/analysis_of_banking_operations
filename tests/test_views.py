import json
from typing import Any

import pandas as pd  # type: ignore
import pytest

import src.views as views


def test_build_home_payload_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    # Заглушаем внешние API
    monkeypatch.setattr(views, "get_currency_rates", lambda codes: [{"currency": c, "rate": 1.23} for c in codes])
    monkeypatch.setattr(views, "get_stock_prices", lambda codes: [{"stock": c, "price": 100.0} for c in codes])

    settings = {"user_currencies": ["USD", "EUR"], "user_stocks": ["AAPL"]}
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-16", "2021-12-20", "2021-12-21"]),
            "amount": [-100.0, -200.0, 50.0],
            "category": ["ЖКХ", "Еда", "Бонусы"],
            "description": ["", "", ""],
            "card": ["VISA 1111", "VISA 1111", "VISA 2222"],
        }
    )
    at = pd.Timestamp("2021-12-20 12:00:00")

    payload = views._build_home_payload(df, at, settings)

    assert payload["greeting"] == "Добрый день"

    cards = {c["last_digits"]: c for c in payload["cards"]}
    assert "1111" in cards and "2222" in cards
    assert cards["1111"]["total_spent"] == 300.0 and cards["1111"]["cashback"] == 3.0
    assert cards["2222"]["total_spent"] == 0.0 and cards["2222"]["cashback"] == 0.0

    order = [c["last_digits"] for c in payload["cards"]]
    assert order == sorted(order)

    top = payload["top_transactions"]
    assert len(top) == 2
    assert top[0]["amount"] == -200.0 and top[1]["amount"] == -100.0

    assert payload["currency_rates"] == [{"currency": "USD", "rate": 1.23}, {"currency": "EUR", "rate": 1.23}]
    assert payload["stock_prices"] == [{"stock": "AAPL", "price": 100.0}]


def test_build_home_payload_empty_month_top(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(views, "get_currency_rates", lambda codes: [])
    monkeypatch.setattr(views, "get_stock_prices", lambda codes: [])
    settings: dict[str, Any] = {"user_currencies": [], "user_stocks": []}

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-01-05", "2022-01-10"]),
            "amount": [-10.0, -20.0],
            "category": ["A", "B"],
            "description": ["", ""],
            "card": ["VISA 0001", "VISA 0002"],
        }
    )
    at = pd.Timestamp("2021-12-20 12:00:00")
    payload = views._build_home_payload(df, at, settings)
    assert payload["top_transactions"] == []


def test_build_home_payload_cashback_rounding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(views, "get_currency_rates", lambda codes: [])
    monkeypatch.setattr(views, "get_stock_prices", lambda codes: [])
    settings: dict[str, Any] = {"user_currencies": [], "user_stocks": []}

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-10"]),
            "amount": [-100.555],
            "category": ["X"],
            "description": [""],
            "card": ["VISA 9999"],
        }
    )
    at = pd.Timestamp("2021-12-15 08:00:00")
    payload = views._build_home_payload(df, at, settings)
    card = next(c for c in payload["cards"] if c["last_digits"] == "9999")
    assert card["total_spent"] == 100.56
    assert card["cashback"] == 1.01


def test_home_view_integration_mocks_io(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-16", "2021-12-20"]),
            "amount": [-10.0, 20.0],
            "category": ["A", "B"],
            "description": ["", ""],
            "card": ["VISA 1234", "VISA 5678"],
        }
    )
    monkeypatch.setattr(views, "read_user_settings", lambda *_: {"user_currencies": ["USD"], "user_stocks": ["AAPL"]})
    monkeypatch.setattr(views, "load_transactions_xlsx", lambda *_: df)
    monkeypatch.setattr(views, "get_currency_rates", lambda codes: [{"currency": c, "rate": 77.7} for c in codes])
    monkeypatch.setattr(views, "get_stock_prices", lambda codes: [{"stock": c, "price": 123.45} for c in codes])

    out = views.home_view("2021-12-20 09:00:00", "ignored.xlsx")
    payload = json.loads(out)

    assert payload["greeting"] in {"Доброе утро", "Добрый день", "Добрый вечер", "Доброй ночи"}
    assert payload["currency_rates"][0]["rate"] == 77.7
    assert payload["stock_prices"][0]["price"] == 123.45
    assert any(c["last_digits"] == "1234" for c in payload["cards"])


def test_home_view_with_df(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-20"]),
            "amount": [-50.0],
            "category": ["Еда"],
            "description": [""],
            "card": ["VISA 1111"],
        }
    )
    monkeypatch.setattr(views, "read_user_settings", lambda *_: {"user_currencies": [], "user_stocks": []})
    monkeypatch.setattr(views, "get_currency_rates", lambda codes: [])
    monkeypatch.setattr(views, "get_stock_prices", lambda codes: [])

    out = views.home_view_with_df(df, "2021-12-20 18:00:00")
    payload = json.loads(out)
    assert payload["greeting"] == "Добрый вечер"
    assert payload["cards"][0]["last_digits"] == "1111"
