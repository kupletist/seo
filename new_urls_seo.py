import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import gspread
import pandas as pd
import os
import json
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
from datetime import datetime, timedelta as td
from git import Repo
import csv
from github import Github

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'kommerssc-f064e012adc1.json'
property_id = '341387871'

credentials_path = 'kommerssc-d113d5ba4912.json'
credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=['https://www.googleapis.com/auth/webmasters.readonly'])
service = build('webmasters', 'v3', credentials=credentials)

def main():
    today = datetime.today()
    today2 = today.date()
    yesterday = (today - td(days=1)).date()
    pubdate1 = (today - td(days=3)).date()
    # pubdate2 = today2

    start = str(yesterday)
    end = str(yesterday)

    client = BetaAnalyticsDataClient()
    property_id = '341387871'
    property_str = f"properties/{property_id}"

    request = RunReportRequest(
        property=property_str,
        dimensions=[
            Dimension(name="unifiedPagePathScreen"),
            # Dimension(name="pageLocation"),
            # Dimension(name="unifiedScreenClass"),
        ],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimension_filter=FilterExpression(and_group=FilterExpressionList(expressions=[
            FilterExpression(
                filter=Filter(
                    field_name='customEvent:date',
                    string_filter=Filter.StringFilter(
                        value=f'{pubdate1}',
                        match_type=Filter.StringFilter.MatchType.EXACT
                    )
                )
            ),
            FilterExpression(
                filter=Filter(
                    field_name='sessionSourceMedium',
                    string_filter=Filter.StringFilter(
                        value=f'organic',
                        match_type=Filter.StringFilter.MatchType.CONTAINS
                    )
                )
            )
        ])),
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)]
    )

    response = client.run_report(request)

    page_path_l = []
    url_l = []
    views_l = []

    for row in response.rows[:100]:
        page_path = row.dimension_values[0].value
        try:
            page_path = page_path.split('/')[2]
        except:
            page_path = 'rubric/5'
        page_path_l.append(page_path)
        url = f'https://www.kommersant.ru{row.dimension_values[0].value}'
        url_l.append(url)
        views = row.metric_values[0].value
        views_l.append(views)



    df = pd.DataFrame({'id': page_path_l, 'url': url_l, 'views': views_l})

    imp_list = []
    clicks_list = []
    pos_list = []
    ctr_list = []

    for i in page_path_l[:100]:
        payload = {
            'startDate': f'{yesterday}',
            'endDate': f'{yesterday}',
            'dimensions': ["PAGE"],
            'rowLimit': 100,
            'startRow': 0,
            'dimensionFilterGroups': [{
                'filters': [{
                    'dimension': 'page',
                    'operator': 'contains',
                    'expression': f'{i}'
                }]
            }],
            'dataState': 'all'
        }
        site_url = "sc-domain:kommersant.ru"

        response = service.searchanalytics().query(siteUrl=site_url, body=payload).execute()
        # data = r.json()
        # print(json.dumps(response, indent=4, ensure_ascii=False))

        if 'rows' in response:
            for row in response['rows']:
                impressions = row['impressions']
                imp_list.append(impressions)
                clicks = row['clicks']
                clicks_list.append(clicks)
                ctr = round(row['ctr'] *100, 1)
                ctr_list.append(ctr)
                position = round(row['position'], 1)
                pos_list.append(position)
        else:
            imp_list.append(0)
            clicks_list.append(0)
            ctr_list.append(0.0)
            pos_list.append(0.0)
    df['impressions'] = imp_list[:100]
    df['clicks'] = clicks_list[:100]
    df['ctr'] = ctr_list[:100]
    df['position'] = pos_list[:100]
    df = df.drop_duplicates(subset='id', keep='first')

    df.to_csv('new_urls_seo.csv', encoding='utf-8', index=False)

    # Путь к локальному репозиторию
    repo_path = '/seo_serv'

    # Инициализация репозитория
    repo = Repo(repo_path)

    # Добавление файла csv
    file_path = '/seo_serv/new_urls_seo.csv'
    repo.index.add([file_path])

    # Создание коммита
    repo.index.commit('Добавлен файл new_urls_seo.csv')

    # Отправка изменений на удаленный репозиторий
    origin = repo.remote('origin')
    origin.push()

# настройка расписания
schedule.every().day.at("22:35", "Europe/Moscow").do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()
