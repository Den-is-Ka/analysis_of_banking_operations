import json
import pytest

from src.services import best_cashback_categories, CASHBACK_RATE


def test_best_cashback_basic_aggregation_and_month_filter():
    data = [
        {"date": "2021-12-05", "amount": -100, "category": "Еда"},
        {"date": "2021-12-10", "amount": -200.49, "category": "Еда"},
        {"date": "2021-12-15", "amount": -50, "category": "Транспорт"},
        {"date": "2021-11-30", "amount": -999, "category": "Еда"},  # другой месяц → игнор
        {"date": "2021-12-01", "amount": 300, "category": "Пополнения"},  # доход → игнор
    ]
    s = best_cashback_categories(data, 2021, 12)
    out = json.loads(s)

    # Еда: (100 + 200.49) * 0.05 = 15.0245 -> 15.02
    assert out["Еда"] == round((100 + 200.49) * CASHBACK_RATE, 2)
    # Транспорт: 50 * 0.05 = 2.5
    assert out["Транспорт"] == round(50 * CASHBACK_RATE, 2)


def test_best_cashback_logs_broken_rows(caplog):
    # одна строка битая (не дата / не число), одна валидная
    data = [
        {"date": "не дата", "amount": "abc", "category": "Еда"},
        {"date": "2021-12-20", "amount": -100, "category": "Еда"},
    ]
    with caplog.at_level("ERROR"):
        s = best_cashback_categories(data, 2021, 12)
    out = json.loads(s)
    assert out["Еда"] == round(100 * CASHBACK_RATE, 2)
    # зафиксирован лог об исключении
    assert any("Skip broken tx" in rec.message for rec in caplog.records)


def test_best_cashback_missing_category_defaults_to_unknown():
    data = [{"date": "2021-12-10", "amount": -40}]  # без category
    s = best_cashback_categories(data, 2021, 12)
    out = json.loads(s)
    assert out["Неизвестно"] == round(40 * CASHBACK_RATE, 2)


def test_best_cashback_json_encoding_and_indent():
    data = [{"date": "2021-12-01", "amount": -100, "category": "Еда"}]
    s = best_cashback_categories(data, 2021, 12)
    # ensure_ascii=False → русские буквы не экранируются
    assert "Еда" in s and "\\u" not in s
    # это точно JSON-строка, которую можно распарсить
    out = json.loads(s)
    assert isinstance(out, dict) and out["Еда"] == round(100 * CASHBACK_RATE, 2)
