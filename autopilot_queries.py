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
    seven_before = (today - timedelta(days=7)).date()
    # threedays_before = (today - timedelta(days=3)).date()
    # threemonths_before = (today - timedelta(days=90)).date()

    start = str(seven_before)
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
            #
            # if 'gallery' in data['page']:
                # Применение фильтров
            # if 5 <= data['position'] <= 15 and data['impressions'] >= 1 and 0 <= data['ctr'] <= 4:
            if all(keyword not in data['query'] for keyword in ['автомобиль', 'Автопилот', 'autopilot.ru', 'автопилот', 'журнал автопилот']):
                results.append(data)
        # Сортировка по показам
        results.sort(key=lambda x: x['impressions'], reverse=True)

        return pd.DataFrame.from_dict(results)

    payload = {
        'startDate': f'{start}',
        'endDate': f'{end}',
        'dimensions': ["query"],
        'rowLimit': 3000,
        'startRow': 0,
        'dataState': 'all'
    }

    site_url = "sc-domain:autopilot.ru"

    df = query(service, site_url, payload)

    df = df.head(200)

    # df_filtered = df.dropna()  # Удаление строк с отсутствующими значениями
    df_filtered = df[['query', 'impressions', 'clicks', 'ctr', 'position']]  # Выбор нужных столбцов

    df_filtered = df_filtered.astype({'query': str, 'impressions': int, 'clicks': int, 'ctr': float, 'position': int})

    df_filtered.to_csv('autopilot_queries.csv', index=False)

    # Путь к локальному репозиторию
    repo_path = '/seo_serv'

    # Инициализация репозитория
    repo = Repo(repo_path)

    # Добавление файла fresh_queries.csv
    file_path = '/seo_serv/autopilot_queries.csv'
    repo.index.add([file_path])

    # Создание коммита
    repo.index.commit('Добавлен файл autopilot_queries.csv')

    # Отправка изменений на удаленный репозиторий
    origin = repo.remote('origin')
    origin.push()

    print("готово, запросы за неделю выгрузились (autopilot_queries)")

# настройка расписания
schedule.every().monday.at("08:00", "Europe/Moscow").do(main)
# schedule.every(3).hours.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()

