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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/seo/kommerssc-8d5b7406facf.json'
property_id = '341387871'

credentials_path = '/home/seo/kommerssc-2be9c1f40b77.json'
credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=['https://www.googleapis.com/auth/webmasters.readonly'])
service = build('webmasters', 'v3', credentials=credentials)

# Запрос на получение списка сайтов
sites_list = service.sites().list().execute()


# Вывод списка сайтов
for site in sites_list['siteEntry']:
    print(site['siteUrl'])

def main():

    today = datetime.today()
    today2 = today.date()
    yesterday = (today - timedelta(days=1)).date()
    twodays_before = (today - timedelta(days=2)).date()
    print(twodays_before)
    print(today2)
    # start = yesterday
    # end = today2

    start = str(twodays_before)
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
            if 5 <= data['position'] <= 10 and data['impressions'] >= 3000 and 0 <= data['ctr'] <= 4:
                results.append(data)
        # Сортировка по показам
                results.sort(key=lambda x: x['impressions'], reverse=True)

        return pd.DataFrame.from_dict(results)

    payload = {
        'startDate': f'{start}',
        'endDate': f'{end}',
        'dimensions': ["page"],
        'rowLimit': 3000,
        'startRow': 0
    }

    site_url = "sc-domain:kommersant.ru"

    df = query(service, site_url, payload)

    # Добавьте этот код после определения функции query

    # df_filtered = df.dropna()  # Удаление строк с отсутствующими значениями
    df_filtered = df[['page', 'impressions', 'ctr', 'position']]  # Выбор нужных столбцов
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

    df_filtered['заголовок'] = pd.Series(titles_list + [''] * (len(df_filtered) - len(titles_list)), index=df_filtered.index)
    # df_filtered['заголовок'] = titles_list
    df_filtered = df_filtered.astype({'impressions': int, 'position': int, 'заголовок': str})
    print(df_filtered)

    df_filtered.to_csv('seo_titles.csv', index=False)

    # filename = 'seo_titles.csv'
    # with open(filename, 'w', newline='', encoding='utf-8') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(df_filtered)

    # Путь к локальному репозиторию
    repo_path = '/home/seo'
    
    # Инициализация репозитория
    repo = Repo(repo_path)
    
    # Добавление файла seo_titles.csv
    file_path = '/home/seo/seo_titles.csv'
    repo.index.add([file_path])
    
    # Создание коммита
    repo.index.commit('Добавлен файл seo_titles.csv')
    
    # Отправка изменений на удаленный репозиторий
    origin = repo.remote('origin')
    origin.push()
    
    print("готово")

main()

# # настройка расписания
# schedule.every().day.at("06:00", "Europe/Moscow").do(main)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

if __name__ == "__main__":
   main()