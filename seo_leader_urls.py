import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import gspread
import pandas as pd
import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import BatchRunReportsRequest
from google.analytics.data_v1beta.types import NumericValue
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import Pivot
from google.analytics.data_v1beta.types import RunPivotReportRequest
from google.analytics.data_v1beta.types import OrderBy
import gspread
import schedule
import time
import pickle
from datetime import datetime, timedelta
from git import Repo
import csv
from github import Github

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/seo_serv/kommerssc-f064e012adc1.json'
property_id = '341387871'

credentials_path = '/seo_serv/kommerssc-d113d5ba4912.json'
credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=[
    'https://www.googleapis.com/auth/webmasters.readonly'])
service = build('webmasters', 'v3', credentials=credentials)

# Запрос на получение списка сайтов
sites_list = service.sites().list().execute()


# Вывод списка сайтов
# for site in sites_list['siteEntry']:
# print(site['siteUrl'])

def main():
    today = datetime.today()
    today2 = today.date()
    yesterday = (today - timedelta(days=1)).date()
    twomonth_before = (today - timedelta(days=60)).date()

    start = str(twomonth_before)
    end = str(today2)

    def query(service, site_url, payload):
        response = service.searchanalytics().query(siteUrl=site_url, body=payload).execute()
        results = []

        for row in response['rows']:
            data = {}
            for i in range(len(payload['dimensions'])):
                data[payload['dimensions'][i]] = row['keys'][i]
            data['impressions'] = row['impressions']
            data['clicks'] = row['clicks']
            data['ctr'] = round(row['ctr'] * 100, 2)
            data['position'] = round(row['position'], 2)

            # Применение фильтров
            # if 10 <= data['position'] <= 15 and 5000 <= data['impressions'] and data['impressions'] <= 40000 and 0 <= data['ctr'] <= 3:
            # if 10 <= data['position'] <= 20 and 10000 <= data['impressions'] and 0 <= data['ctr'] <= 2:
            if 'doc' in data['page'] or 'gallery' in data['page']:
                results.append(data)
                # Сортировка по показам
            results.sort(key=lambda x: x['impressions'], reverse=True)

        return pd.DataFrame.from_dict(results)

    payload = {
        'startDate': f'{start}',
        'endDate': f'{end}',
        'dimensions': ["page"],
        'rowLimit': 3000,
        'startRow': 0,
        'dataState': 'all'
    }

    site_url = "sc-domain:kommersant.ru"

    df = query(service, site_url, payload)
    df = df.head(400)

    # Добавьте этот код после определения функции query

    # df_filtered = df.dropna()  # Удаление строк с отсутствующими значениями
    df_filtered = df[['page', 'impressions', 'clicks', 'ctr', 'position']]  # Выбор нужных столбцов
    # df_filtered = df_filtered.astype({'impressions': int, 'position': int})  # Приведение типов данных
    # df_filtered = df_filtered.astype(str)

    urls_list = df_filtered['page'].tolist()
    dimensions_list = []

    for i in urls_list:
        client = BetaAnalyticsDataClient()
        property_id = '341387871'
        property_str = f"properties/{property_id}"

        request = RunReportRequest(
            property=property_str,
            dimensions=[
                Dimension(name="pageTitle"),
                Dimension(name="pageLocation"),
                # Dimension(name="unifiedScreenClass"),
            ],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimension_filter=FilterExpression(and_group=FilterExpressionList(expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name='pageLocation',
                        string_filter=Filter.StringFilter(
                            value=f'{i}',
                            match_type=Filter.StringFilter.MatchType.EXACT
                        )
                    )
                )
            ]))
        )

        response = client.run_report(request)

        # Добавление dimensions и их values в список
        for dimension in response.dimension_headers:
            dimensions_list.append({
                'name': dimension.name,
                'values': [row.dimension_values[0].value for row in response.rows]
            })

    titles_list = []
    for dimension in dimensions_list:
        # print(f"Dimension: {dimension['name']}")
        names = tuple(dimension['values'])
        titles_list.append(names)

    titles_list = list(dict.fromkeys(titles_list))  # убираем из списка дубли

    df_filtered['заголовок'] = pd.Series(titles_list + [''] * (len(df_filtered) - len(titles_list)),
                                         index=df_filtered.index)
    # df_filtered['заголовок'] = titles_list
    df_filtered = df_filtered.astype({'impressions': int, 'clicks': int, 'ctr': float, 'position': int, 'заголовок': str})

    df_filtered.to_csv('seo_leader_urls.csv', index=False)

    # Путь к локальному репозиторию
    repo_path = '/seo_serv'

    # Инициализация репозитория
    repo = Repo(repo_path)

    # Добавление файла seo_leader_urls.csv
    file_path = '/seo_serv/seo_leader_urls.csv'
    repo.index.add([file_path])

    # Создание коммита
    repo.index.commit('Добавлен файл seo_leader_urls.csv')

    # Отправка изменений на удаленный репозиторий
    origin = repo.remote('origin')
    origin.push()

    print("готово, выгрузились топ-seo-урлы за посл 60 дней (seo_leader_urls)")


main()

# # настройка расписания
# schedule.every().day.at("08:00", "Europe/Moscow").do(main)
schedule.every().monday.at("09:15").do(main)
# # schedule.every(3).hours.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()
