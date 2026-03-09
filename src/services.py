from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Ставка кешбэка (пример: 5%).
CASHBACK_RATE = 0.05


def best_cashback_categories(data: list[dict[str, Any]], year: int, month: int) -> str:
    """Считает потенциал кешбэка по категориям за указанный месяц.
    data: список транзакций со строковыми ключами: "date", "amount", "category"
    Возвращает JSON-строку: {"Категория": сумма_кешбэка, ...}
    """
    # Фильтрация по месяцу
    result: dict[str, float] = {}
    for tx in data:
        try:
            dt = datetime.fromisoformat(str(tx["date"]))
            if dt.year == year and dt.month == month:
                amount = float(tx["amount"])
                if amount < 0:  # учитываем только расходы
                    cat = str(tx.get("category", "Неизвестно"))
                    spent = -amount
                    result[cat] = result.get(cat, 0.0) + spent * CASHBACK_RATE
        except Exception as e:
            logger.exception("Skip broken tx: %s", e)

    # Округление до 2 знаков
    rounded = {k: round(v, 2) for k, v in result.items()}
    return json.dumps(rounded, indent=4, ensure_ascii=False)
