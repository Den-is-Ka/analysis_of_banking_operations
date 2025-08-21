from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, cast

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv
# import xlrd  # noqa: F401

load_dotenv()
logger = logging.getLogger(__name__)

DATE_FMT_IN = "%Y-%m-%d %H:%M:%S"
DATE_FMT_OUT = "%d.%m.%Y"

EXPECTED_COLUMNS = {"date", "amount", "category", "description", "card"}

DEFAULT_SETTINGS = {
    "user_currencies": ["USD", "EUR"],
    "user_stocks": ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"],
}


def parse_dt(dt_str: str) -> datetime:
    """Парсим строку вида 'YYYY-MM-DD HH:MM:SS' в datetime."""
    return datetime.strptime(dt_str, DATE_FMT_IN)


def format_date(d: pd.Timestamp | datetime) -> str:
    """Форматируем дату в вид DD.MM.YYYY (для JSON-ответов)."""
    return str(pd.Timestamp(d).strftime(DATE_FMT_OUT))


def human_greeting(dt: datetime) -> str:
    """Возвращает приветствие по часу времени."""
    h = dt.hour
    if 5 <= h <= 11:
        return "Доброе утро"
    if 12 <= h <= 17:
        return "Добрый день"
    if 18 <= h <= 22:
        return "Добрый вечер"
    return "Доброй ночи"


def last4(card_text: str | None) -> str:
    """Достаёт последние 4 цифры из строки с картой."""
    digits = re.sub(r"\D+", "", card_text or "")
    return digits[-4:] if len(digits) >= 4 else digits


def start_of_month(dt: datetime) -> datetime:
    """Первый день месяца в 00:00:00."""
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def filter_month_to_date(df: pd.DataFrame, end_dt: datetime) -> pd.DataFrame:
    """Фильтр транзакций в диапазоне [первое число месяца .. end_dt] включительно."""
    start = start_of_month(end_dt)
    mask = (df["date"] >= start) & (df["date"] <= end_dt)
    return df.loc[mask].copy()


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_project_path(rel_or_abs: str | Path) -> Path:
    """Возвращаем абсолютный путь от корня проекта."""
    p = Path(rel_or_abs)
    return p if p.is_absolute() else get_project_root() / p


def read_user_settings(path: str = "user_settings.json") -> dict[str, Any]:
    """Читаем настройки. Если файла нет/битый — вернём DEFAULT_SETTINGS (и предупредим в лог)."""
    abs_path = resolve_project_path(path)
    if not abs_path.exists():
        logger.warning(
            "Файл настроек %s не найден — использую значения по умолчанию: %s",
            abs_path,
            DEFAULT_SETTINGS,
        )
        return DEFAULT_SETTINGS.copy()

    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            raw: Any = json.load(f)
    except Exception as e:
        logger.warning("Не удалось прочитать %s (%s) — использую значения по умолчанию", abs_path, e)
        return DEFAULT_SETTINGS.copy()

    # гарантируем словарь
    if not isinstance(raw, dict):
        logger.warning("Настройки в %s не словарь — использую значения по умолчанию", abs_path)
        return DEFAULT_SETTINGS.copy()

    data = cast(dict[str, Any], raw)
    data.setdefault("user_currencies", [])
    data.setdefault("user_stocks", [])
    return data


def get_currency_rates(codes: Iterable[str]) -> list[dict[str, Any]]:
    """
    Возвращает список словарей [{"currency": "USD", "rate": 73.21}, ...].

    Поддержка:
    - apilayer: FX_API_URL указывает на apilayer (или FX_PROVIDER=apilayer). Единый ответ с base и rates.
    - generic: любой URL, принимающий ?symbol=XXX и возвращающий {"rate": ...} (или price/value).
    """
    url = os.getenv("FX_API_URL", "")
    key = os.getenv("FX_API_KEY", "")
    base = (os.getenv("FX_BASE", "RUB") or "RUB").upper()
    provider = (os.getenv("FX_PROVIDER", "auto") or "auto").lower()
    codes = [str(c).upper() for c in codes]

    if not url:
        logger.warning("FX_API_URL не задан — вернём заглушки 0.0")
        return [{"currency": c, "rate": 0.0} for c in codes]

    try:
        if provider == "apilayer" or "apilayer.com/exchangerates_data" in url:
            headers: dict[str, str] = {"apikey": key} if key else {}
            sym = ",".join(sorted(set(codes + ["RUB"])))
            r = requests.get(url, params={"base": base, "symbols": sym}, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            resp_base = str(data.get("base", base)).upper()
            rates: dict[str, Any] = data.get("rates", {}) or {}

            out: list[dict[str, Any]] = []
            if base == "RUB":
                if resp_base == "RUB":
                    for c in codes:
                        v = float(rates.get(c, 0.0) or 0.0)
                        out.append({"currency": c, "rate": (1.0 / v) if v else 0.0})
                else:
                    rub = float(rates.get("RUB", 0.0) or 0.0)
                    for c in codes:
                        v = 1.0 if c == resp_base else float(rates.get(c, 0.0) or 0.0)
                        out.append({"currency": c, "rate": (rub / v) if (rub and v) else 0.0})
                return out

            result: list[dict[str, Any]] = []
            for c in codes:
                v = 1.0 if c == resp_base else float(rates.get(c, 0.0) or 0.0)
                result.append({"currency": c, "rate": v})
            return result

        result = []
        headers = {"Authorization": key} if key else {}
        for c in codes:
            try:
                rr = requests.get(url, params={"symbol": c}, headers=headers, timeout=10)
                rr.raise_for_status()
                d = rr.json()
                rate = float(d.get("rate") or d.get("price") or d.get("value") or 0.0)
            except Exception as e:
                logger.exception("FX rate error for %s: %s", c, e)
                rate = 0.0
            result.append({"currency": c, "rate": rate})
        return result

    except Exception as e:
        logger.exception("FX rates error: %s", e)
        return [{"currency": c, "rate": 0.0} for c in codes]


def get_stock_prices(tickers: Iterable[str]) -> list[dict[str, Any]]:
    """
    Возвращает [{"stock": "AAPL", "price": 150.12}, ...].

    Провайдеры:
    - Twelve Data: STOCKS_API_URL=https://api.twelvedata.com/price (или /quote), ключ -> apikey в query.
      /price: {"price":"<float>"}; /quote: может содержать close/last/c.
    - generic: любой URL с ?symbol=..., ключ в Authorization.
    """
    url = os.getenv("STOCKS_API_URL", "")
    key = os.getenv("STOCKS_API_KEY", "")
    provider = (os.getenv("STOCKS_PROVIDER", "auto") or "auto").lower()
    prepost = os.getenv("STOCKS_PREPOST", "").lower() in ("1", "true", "yes", "on")

    syms = [str(t).upper() for t in tickers]
    if not url:
        logger.warning("STOCKS_API_URL не задан — вернём заглушки 0.0")
        return [{"stock": t, "price": 0.0} for t in syms]

    results: list[dict[str, Any]] = []

    def _clean_ascii(s: str) -> str:
        s = (s or "").strip()
        if s.startswith(("'", '"')) and s.endswith(("'", '"')) and len(s) >= 2:
            s = s[1:-1].strip()
        s = s.replace("«", "").replace("»", "").replace("“", "").replace("”", "").replace("—", "-")
        try:
            s.encode("latin-1")
        except UnicodeEncodeError:
            raise RuntimeError("STOCKS_API_KEY содержит недопустимые символы (не ASCII).")
        return s

    key = _clean_ascii(key)

    for t in syms:
        try:
            if provider == "twelvedata" or "api.twelvedata.com" in url:
                params = {"symbol": t, "apikey": key}
                if prepost:
                    params["prepost"] = "true"
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                d = r.json()

                price = None
                if isinstance(d, dict):
                    if "price" in d:
                        price = float(d["price"])
                    elif "close" in d:
                        price = float(d["close"] or 0.0)
                    elif "last" in d:
                        price = float(d["last"] or 0.0)
                    elif "c" in d:
                        price = float(d["c"] or 0.0)

                    if d.get("status") == "error":
                        logger.warning("Twelve Data error for %s: %s", t, d.get("message"))
                        price = 0.0

                results.append({"stock": t, "price": float(price or 0.0)})
            else:
                headers = {"Authorization": key} if key else None
                r = requests.get(url, params={"symbol": t}, headers=headers, timeout=10)
                r.raise_for_status()
                d = r.json()
                price = float(d.get("price") or d.get("last") or d.get("close") or d.get("c") or 0.0)
                results.append({"stock": t, "price": price})
        except Exception as e:
            logger.exception("Stock price error for %s: %s", t, e)
            results.append({"stock": t, "price": 0.0})

    return results


def load_transactions_xlsx(path: str | Path) -> pd.DataFrame:
    """
    Читает Excel/CSV с транзакциями и приводит к колонкам:
    date (datetime), amount (float), category (str), description (str), card (str)

    Алгоритм:
    1) Определяем формат по «магии» файла: ZIP ('PK') -> xlsx, OLE -> xls, иначе пробуем как текст (CSV) или xlsx.
    2) Считываем «сырой» DataFrame без заголовков (header=None).
    3) Находим строку‑шапку эвристикой (по ключевым словам), формируем имена колонок.
    4) Мэппим названия колонок к ожидаемым через словарь синонимов + нормализацию.
    5) Подчищаем и приводим типы; отбрасываем битые строки; заполняем пропуски безопасными значениями.
    """
    abs_path = resolve_project_path(path)
    logger.info("Loading Excel: %s", abs_path)
    if not abs_path.exists():
        raise FileNotFoundError(f"Не найден файл с операциями: {abs_path}")

    def sniff(p: Path) -> str:
        with open(p, "rb") as f:
            head = f.read(8)
        if head.startswith(b"PK"):
            return "xlsx"
        if head.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
            return "xls"
        try:
            with open(p, "rb") as f:
                f.read(4096).decode("utf-8")
            return "text"
        except UnicodeDecodeError:
            return "unknown"

    fmt = sniff(abs_path)

    if fmt == "xlsx":
        raw = pd.read_excel(abs_path, engine="openpyxl", header=None)
    elif fmt == "xls":
        try:
            import xlrd  # noqa: F401
        except Exception:
            raise RuntimeError("Это .xls — установи: pip install 'xlrd==1.2.0'")
        raw = pd.read_excel(abs_path, engine="xlrd", header=None)
    elif fmt == "text":
        raw = pd.read_csv(abs_path, sep=None, engine="python", header=None)
    else:
        raw = pd.read_excel(abs_path, engine="openpyxl", header=None)

    def norm(s: str) -> str:
        return re.sub(r"[^a-zа-я0-9]", "", str(s).strip().lower())

    header_row = 0
    best_score = -1
    targets = ["дат", "summ", "sum", "сум", "катег", "опис", "назнач", "card", "карт", "счет", "счёт", "pan"]
    for i in range(min(10, len(raw))):
        names = [norm(x) for x in list(raw.iloc[i].values)]
        score = sum(any(t in n for t in targets) for n in names)
        if score > best_score:
            best_score, header_row = score, i

    df = raw.iloc[header_row + 1 :].copy()
    df.columns = [str(x) for x in raw.iloc[header_row].values]

    synonyms = {
        "date": {"date", "дата", "дата операции", "датаоперации", "дата и время", "date/time", "posting date"},
        "amount": {"amount", "сумма", "сумма операции", "суммаоперации", "списание", "итого", "debit", "credit"},
        "category": {"category", "категория", "категории", "тип операции", "тип"},
        "description": {
            "description",
            "описание",
            "описание операции",
            "назначение платежа",
            "комментарий",
            "merchant",
            "получатель",
        },
        "card": {"card", "карта", "номер карты", "маска карты", "pan", "счет", "счёт", "account", "iban"},
    }

    def build_map(cols: list[str]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        norm_cols = {c: norm(c) for c in cols}
        for need, variants in synonyms.items():
            for c in cols:
                if c.strip().lower() in variants:
                    mapping[c] = need
                    break
            else:
                for c, nc in norm_cols.items():
                    if need == "date" and ("дат" in nc or "date" in nc):
                        mapping[c] = need
                        break
                    if need == "amount" and any(k in nc for k in ["sum", "сум", "amount", "debit", "credit"]):
                        mapping[c] = need
                        break
                    if need == "category" and any(k in nc for k in ["катег", "categ", "type", "тип"]):
                        mapping[c] = need
                        break
                    if need == "description" and any(
                        k in nc for k in ["опис", "назнач", "desc", "comment", "merchant", "получател"]
                    ):
                        mapping[c] = need
                        break
                    if need == "card" and any(k in nc for k in ["карт", "pan", "счет", "счёт", "account", "iban", "card"]):
                        mapping[c] = need
                        break
        return mapping

    mapping = build_map(df.columns.tolist())
    df = df.rename(columns=mapping)

    for col in ("date", "amount", "category", "description", "card"):
        if col not in df.columns:
            if col == "category":
                df[col] = "Неизвестно"
            elif col == "description":
                df[col] = ""
            elif col == "card":
                df[col] = df.get("description", "").astype(str).str.extract(r"(\d{4})").fillna("")
            else:
                pass

    critical_missing = {"date", "amount"} - set(df.columns)
    if critical_missing:
        raise ValueError(
            "Не удалось распознать обязательные колонки: "
            f"{', '.join(sorted(critical_missing))}. "
            f"Найдены колонки: {list(df.columns)}. "
            "Подправь названия столбцов в Excel или расширь словарь синонимов."
        )

    sample = df["date"].astype(str).head(20)
    use_dayfirst = sample.str.contains(r"\b\d{1,2}\.\d{1,2}\.\d{2,4}\b").mean() > 0.3
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=use_dayfirst)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    for c in ("category", "description", "card"):
        if c in df.columns:
            df[c] = df[c].astype(str)

    df = df.dropna(subset=["date", "amount"]).reset_index(drop=True)

    for c in ("category", "description", "card"):
        if c not in df.columns:
            df[c] = "" if c != "category" else "Неизвестно"

    missing_final = EXPECTED_COLUMNS - set(df.columns)
    if missing_final:
        logger.warning("Отсутствуют необязательные столбцы: %s", missing_final)

    return df[["date", "amount", "category", "description", "card"]]
