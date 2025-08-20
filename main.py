from __future__ import annotations

import json
import logging
from argparse import ArgumentParser
from datetime import datetime


from src.reports import save_report, spending_by_category
from src.services import best_cashback_categories
from src.utils import load_transactions_xlsx
from src.views import home_view

logging.basicConfig(level=logging.INFO)


def run_home(dt: str, file: str) -> None:
    print(home_view(dt, file))


def run_best(year: int, month: int, file: str) -> None:
    df = load_transactions_xlsx(file)
    data = df[["date", "amount", "category"]].copy()
    data["date"] = data["date"].dt.strftime("%Y-%m-%d")
    payload = json.loads(best_cashback_categories(data.to_dict(orient="records"), year, month))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


@save_report()
def run_report(category: str, dt: str | None, file: str):
    df = load_transactions_xlsx(file)
    return spending_by_category(df, category, dt)


def main():
    ap = ArgumentParser(description="Transactions toolkit")
    sub = ap.add_subparsers(dest="cmd")

    # home
    h = sub.add_parser("home", help="Главная страница")
    h.add_argument("--dt", required=False, help="YYYY-MM-DD HH:MM:SS (по умолчанию текущее время)")
    h.add_argument("--file", default="data/operations.xlsx")

    # best
    b = sub.add_parser("best", help="Лучшие категории кешбэка")
    b.add_argument("--year", type=int, required=True)
    b.add_argument("--month", type=int, required=True)
    b.add_argument("--file", default="data/operations.xlsx")

    # report
    r = sub.add_parser("report", help="Отчёт по категории")
    r.add_argument("--category", required=True)
    r.add_argument("--dt", required=False, help="YYYY-MM-DD")
    r.add_argument("--file", default="data/operations.xlsx")

    args = ap.parse_args()

    if args.cmd == "home":
        dt = args.dt or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        run_home(dt, args.file)

    elif args.cmd == "best":
        run_best(args.year, args.month, args.file)

    elif args.cmd == "report":
        run_report(args.category, args.dt, args.file)

    else:

        df = load_transactions_xlsx("data/operations.xlsx")

        last_dt = df["date"].max().strftime("%Y-%m-%d %H:%M:%S")

        from src.views import home_view_with_df

        print(home_view_with_df(df, last_dt, "user_settings.json"))


if __name__ == "__main__":
    main()
