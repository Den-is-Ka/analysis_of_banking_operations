import json

from config import PATH_TRANSACTIONS, PATH_USER_SETTINGS
from datetime import datetime

def get_datetime_info(date):
    greetins = get_greetings()

    all_transactions = read_data_as_df(PATH_TRANSACTIONS)
    end_period = datetime.strptime(date, "%Y-%m")
    start_period = end_period.replace(day=1, )

    selected_transactions = all_transactions[(pd.to_datetime(all_transactions['Дата операции']) >=start_period) & ()]
    card_info = get_card_info(selected_transactions)
    top_five = get_top_five_max_prices(selected_transactions)

    stock_currencies = get_user_settings(PATH_USER_SETTINGS)
    stocks = stock_currencies['user_stocks']
    currencies = stock_currencies['user_stocks']

    result = {
        "greeting": greetins,
        "card": card_info
    }
    return json.dumps(result, indent=4, ensure_ascii=False)
















