from src.views import get_datetime_info
from src.services import get_search_simple


def main():
    datetime_result = get_datetime_info("2021-01-01 01:02:03")
    print(datetime_result)

    all_transactions = read_data_as_df(PATH_TRANSACTIONS)
    transactions_for_service = all_transactions.to_dict('records')
    service_result = get_search_simple(transactions_for_service,'Перевод')
    print(service_result)

    report_result = spending_by_category(all_transactions, 'Фастфуд', "2021-01-01 01:02:03")
    print(report_result)




if __name__ =='__main__':
    main()
