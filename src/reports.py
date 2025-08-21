from __future__ import annotations

import json
import logging
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd  # type: ignore

logger = logging.getLogger(__name__)


def save_report(filename: Optional[str] = None) -> Callable:
    """Декоратор: сохраняет результат функции-отчёта в файл.
    - Если возвращён DataFrame -> .xlsx
    - Если dict/list -> .json
    - Если строка -> как есть (в текстовый файл)
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            res = func(*args, **kwargs)

            fname = filename
            if not fname:
                fname = f"report_{func.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            path = Path(fname)
            try:
                if isinstance(res, pd.DataFrame):
                    if path.suffix.lower() != ".xlsx":
                        path = path.with_suffix(".xlsx")
                    res.to_excel(path, index=False)
                elif isinstance(res, (dict, list)):
                    if path.suffix.lower() != ".json":
                        path = path.with_suffix(".json")
                    path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
                else:
                    path.write_text(str(res), encoding="utf-8")
                logger.info("Report saved: %s", path)
            except Exception as e:
                logger.exception("Failed to save report %s: %s", path, e)

            return res

        return wrapper

    return decorator


def _last_3_months_range(ref: datetime) -> tuple[datetime, datetime]:
    """Возвращает (start, end) для последних 3 месяцев включительно относительно ref."""
    month = ref.month
    year = ref.year
    start_month = ((month - 2 - 1) % 12) + 1
    year_delta = (month - 2 - 1) // 12
    start_year = year + year_delta
    start = datetime(start_year, start_month, 1)
    return start, ref


def spending_by_category(transactions: pd.DataFrame, category: str, date: Optional[str] = None) -> pd.DataFrame:
    """Возвращает траты по категории за последние 3 месяца, агрегированные по месяцам.
    Вход: DataFrame с колонками: date (datetime), amount (float), category (str)
    Выход: DataFrame с колонками: month (YYYY-MM), spent (float)
    """
    ref = datetime.fromisoformat(date) if date else datetime.now()
    start, end = _last_3_months_range(ref)

    df = transactions.copy()
    df = df[(df["date"] >= start) & (df["date"] <= end) & (df["category"].astype(str) == category)]

    if df.empty:
        return pd.DataFrame({"month": [], "spent": []})

    df["spent"] = df["amount"].apply(lambda x: -x if x < 0 else 0.0)
    df["month"] = df["date"].dt.strftime("%Y-%m")
    agg = df.groupby("month", as_index=False)["spent"].sum()
    return agg.sort_values("month")
