from datetime import datetime
from pathlib import Path

import pandas as pd  # type: ignore
import pytest

from src import utils


def test_load_transactions_csv_with_header_sniff_and_dayfirst(tmp_path: Path) -> None:
    # CSV с "мусорной" строкой, затем шапка по-русски и данные; дата в формате дд.мм.гггг
    csv = tmp_path / "ops.csv"
    rows = [
        ["Это не шапка", "", "", "", ""],
        ["Дата", "Сумма", "Категория", "Описание", "Карта"],
        ["31.12.2021 12:34:56", "-100.50", "Еда", "Магнит", "VISA 1111"],
        ["01.01.2022 08:00:00", "200.00", "Пополнения", "Пополнение", "MC 2222"],
    ]
    import csv as _csv

    with csv.open("w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerows(rows)

    df = utils.load_transactions_xlsx(csv)
    assert list(df.columns) == ["date", "amount", "category", "description", "card"]
    assert df.loc[0, "date"].month == 12 and df.loc[0, "date"].day == 31
    assert float(df.loc[0, "amount"]) == -100.50
    assert df.loc[0, "category"] == "Еда"
    assert df.loc[0, "card"].endswith("1111")


def test_load_transactions_missing_critical_raises(tmp_path: Path) -> None:
    csv = tmp_path / "bad.csv"
    csv.write_text("Дата,Категория,Описание,Карта\n31.12.2021 12:00:00,Еда,ООО,VISA 1111", encoding="utf-8")
    with pytest.raises(ValueError):
        utils.load_transactions_xlsx(csv)


def test_resolve_project_path_absolute_passthrough(tmp_path: Path) -> None:
    abs_p = tmp_path / "abc.txt"
    assert utils.resolve_project_path(abs_p) == abs_p


def test_read_user_settings_default_when_missing(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    data = utils.read_user_settings(str(missing))
    assert "user_currencies" in data and "user_stocks" in data


def test_read_user_settings_corrupted_returns_default(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not-json", encoding="utf-8")
    data = utils.read_user_settings(str(bad))
    assert "user_currencies" in data and "user_stocks" in data


def test_helpers_parse_and_format_and_greeting_and_last4() -> None:
    dt = utils.parse_dt("2021-12-20 13:45:00")
    assert utils.format_date(dt) == "20.12.2021"
    assert utils.human_greeting(datetime(2021, 1, 1, 6, 0, 0)) == "Доброе утро"
    assert utils.human_greeting(datetime(2021, 1, 1, 13, 0, 0)) == "Добрый день"
    assert utils.human_greeting(datetime(2021, 1, 1, 20, 0, 0)) == "Добрый вечер"
    assert utils.human_greeting(datetime(2021, 1, 1, 2, 0, 0)) == "Доброй ночи"
    assert utils.last4("VISA 1234") == "1234"
    assert utils.last4("****5678") == "5678"


def test_filter_month_to_date() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-01", "2021-12-15", "2022-01-05"]),
            "amount": [-1, -2, -3],
            "category": ["A", "B", "C"],
            "description": ["", "", ""],
            "card": ["x", "y", "z"],
        }
    )
    out = utils.filter_month_to_date(df, datetime(2021, 12, 20, 12, 0, 0))
    assert len(out) == 2
