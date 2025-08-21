import os
import json
from types import SimpleNamespace
from datetime import datetime

from typing import Any, Optional
from pathlib import Path
import pandas as pd  # type: ignore
import pytest

import src.utils as utils


def test_parse_format_greeting_last4_and_filter() -> None:
    dt = utils.parse_dt("2021-12-20 06:30:00")
    assert isinstance(dt, datetime)
    assert utils.format_date(dt) == "20.12.2021"
    assert utils.human_greeting(utils.parse_dt("2021-12-20 06:00:00")) == "Доброе утро"
    assert utils.human_greeting(utils.parse_dt("2021-12-20 13:00:00")) == "Добрый день"
    assert utils.human_greeting(utils.parse_dt("2021-12-20 19:00:00")) == "Добрый вечер"
    assert utils.human_greeting(utils.parse_dt("2021-12-20 02:00:00")) == "Доброй ночи"
    assert utils.last4("VISA 1234") == "1234"
    assert utils.last4("**** 567") == "567"

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-01", "2021-12-20", "2022-01-01"]),
            "amount": [1, 2, 3],
            "category": ["A", "B", "C"],
            "description": ["", "", ""],
            "card": ["x", "y", "z"],
        }
    )
    end = utils.parse_dt("2021-12-20 12:00:00")
    month_df = utils.filter_month_to_date(df, end)
    assert len(month_df) == 2


def test_read_user_settings_missing_uses_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    out = utils.read_user_settings("user_settings.json")
    assert out["user_currencies"] == ["USD", "EUR"]
    assert "user_stocks" in out


def test_read_user_settings_valid_and_setdefaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    p = tmp_path / "user_settings.json"
    p.write_text(json.dumps({"user_currencies": ["USD"]}, ensure_ascii=False), encoding="utf-8")
    out = utils.read_user_settings("user_settings.json")
    assert out["user_currencies"] == ["USD"]
    assert "user_stocks" in out


class DummyResp:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._p = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("status error")

    def json(self) -> Any:
        return self._p


def test_get_currency_rates_apilayer_cross_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["FX_PROVIDER"] = "apilayer"
    os.environ["FX_API_URL"] = "https://api.apilayer.com/exchangerates_data/latest"
    os.environ["FX_API_KEY"] = "abc123"
    os.environ["FX_BASE"] = "RUB"

    def fake_get(url: str, params: dict[str, Any], headers: Optional[dict[str, str]] = None, timeout: float = 10,) -> Any:
        assert "symbols" in params and "RUB" in params["symbols"]
        payload = {"base": "EUR", "rates": {"USD": 1.25, "RUB": 100.0}}
        return DummyResp(payload)

    monkeypatch.setattr(utils, "requests", SimpleNamespace(get=fake_get))
    out = utils.get_currency_rates(["usd", "eur"])
    got = {x["currency"]: round(x["rate"], 4) for x in out}
    assert got["USD"] == pytest.approx(80.0, rel=1e-4)
    assert got["EUR"] == pytest.approx(100.0, rel=1e-4)


def test_get_currency_rates_generic_headers_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["FX_PROVIDER"] = "generic"
    os.environ["FX_API_URL"] = "https://example/fx"
    os.environ["FX_API_KEY"] = "k"
    calls = {"n": 0}

    def fake_get(url: str, params: dict[str, Any], headers: Optional[dict[str, str]] = None, timeout: float = 10,) -> Any:
        calls["n"] += 1
        if params.get("symbol") == "USD":
            return DummyResp({"rate": 123})
        raise RuntimeError("boom")

    monkeypatch.setattr(utils, "requests", SimpleNamespace(get=fake_get))
    out = utils.get_currency_rates(["USD", "EUR"])
    assert out[0]["rate"] == 123 and out[1]["rate"] == 0.0
    assert calls["n"] == 2


def test_get_stock_prices_twelvedata_price_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["STOCKS_PROVIDER"] = "twelvedata"
    os.environ["STOCKS_API_URL"] = "https://api.twelvedata.com/price"
    os.environ["STOCKS_API_KEY"] = "key"
    os.environ["STOCKS_PREPOST"] = "true"

    def fake_get(url: str, params: dict[str, Any], headers: Optional[dict[str, str]] = None, timeout: float = 10,) -> Any:
        sym = params["symbol"]
        assert params.get("apikey") == "key"
        assert params.get("prepost") == "true"
        if sym == "AAPL":
            return DummyResp({"price": "230.56"})
        return DummyResp({"status": "error", "message": "limit"})

    monkeypatch.setattr(utils, "requests", SimpleNamespace(get=fake_get))
    out = utils.get_stock_prices(["AAPL", "MSFT"])
    d = {x["stock"]: x["price"] for x in out}
    assert d["AAPL"] == pytest.approx(230.56, rel=1e-9)
    assert d["MSFT"] == 0.0


def test_get_stock_prices_generic_and_key_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["STOCKS_PROVIDER"] = "generic"
    os.environ["STOCKS_API_URL"] = "https://example/stocks"
    os.environ["STOCKS_API_KEY"] = '"key-with-quotes"'

    def fake_get(url: str, params: Optional[dict[str, Any]] = None, headers: Optional[dict[str, str]] = None, timeout: float = 10,) -> Any:
        assert headers == {"Authorization": "key-with-quotes"}
        return DummyResp({"last": 42})

    monkeypatch.setattr(utils, "requests", SimpleNamespace(get=fake_get))
    out = utils.get_stock_prices(["GOOGL"])
    assert out == [{"stock": "GOOGL", "price": 42.0}]


def test_load_transactions_xlsx_text_csv_mapping_and_dayfirst(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    csv_path = tmp_path / "ops.csv"
    csv_content = (
        "мусор,мусор,мусор,мусор\n"
        "Дата операции,Сумма операции,Категория,Описание операции\n"
        "15.12.2021,-100.50,Еда,Покупка в магазине 1234\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    df = utils.load_transactions_xlsx(csv_path)
    assert list(df.columns) == ["date", "amount", "category", "description", "card"]
    assert df.loc[0, "date"].strftime("%Y-%m-%d") == "2021-12-15"
    assert df.loc[0, "amount"] == pytest.approx(-100.50, rel=1e-9)
    assert df.loc[0, "card"] == "1234"


def test_load_transactions_xlsx_xlsx_header_row_not_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    xlsx_path = tmp_path / "ops.xlsx"

    rows = [
        ["какой-то мусор", "", "", "", ""],
        ["Дата", "Сумма", "Категория", "Описание", "Карта"],
        ["2021-12-20", "-50.0", "Еда", "Чек", "VISA 1111"],
    ]
    raw = pd.DataFrame(rows)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        raw.to_excel(w, index=False, header=False)

    df = utils.load_transactions_xlsx(xlsx_path)
    assert list(df.columns) == ["date", "amount", "category", "description", "card"]
    assert df.loc[0, "category"] == "Еда"
    assert df.loc[0, "card"].endswith("1111")


def test_load_transactions_xlsx_missing_critical_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    csv_path = tmp_path / "broken.csv"
    csv_content = "Шапка,ещё\n" "Категория,Описание\n" "Еда,Покупка\n"
    csv_path.write_text(csv_content, encoding="utf-8")
    with pytest.raises(ValueError):
        utils.load_transactions_xlsx(csv_path)
