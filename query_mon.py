import requests
import json
import pandas as pd
import os
import gspread
from datetime import datetime, timedelta

today = datetime.today()
today2 = today.date()
yesterday = (today - timedelta(days=1)).date()
start = (today - timedelta(days=15)).date()
end = (today - timedelta(days=2)).date()

user_id = '86798900'
host_id = 'https:www.kommersant.ru:443'
date1 = str(start)
date2 = str(end)

headers = {'Authorization': 'OAuth y0_AgAAAAAFLHI0AAo8CQAAAADoptVSzQZRaNA4S-CecPn9XzrO1zEkpcw',
           'Content-Type': 'application/json; charset=UTF-8'}
params = {
  "offset": 1,
  "limit": 100,
  "device_type_indicator": "ALL",
  "text_indicator": "URL",
  "filters": {
    "statistic_filters": [
      {
        "statistic_field": "IMPRESSIONS",
        "operation": "GREATER_EQUAL",
        "value": "50",
        "from": f'{date1}',
        "to": f'{date2}'
      },
      {
        "statistic_field": "POSITION",
        "operation": "GREATER_EQUAL",
        "value": "4",
        "from": f'{date1}',
        "to": f'{date2}'
      },
      {
        "statistic_field": "POSITION",
        "operation": "LESS_EQUAL",
        "value": "14",
        "from": f'{date1}',
        "to": f'{date2}'
      },
      {
        "statistic_field": "CTR",
        "operation": "LESS_EQUAL",
        "value": "3.0",
        "from": f'{date1}',
        "to": f'{date2}'
      }
    ]
  }
}

query_monitor = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/query-analytics/list'

r = requests.post(query_monitor, headers=headers, json=params)
data = r.json()
# print(json.dumps(data, indent=4, ensure_ascii=False))

urls = []
for i in data['text_indicator_to_statistics']:
  urls_list = i['text_indicator']['value']
  urls.append(urls_list)

whole_urls = []
for url in urls:
  whole_url = 'https://www.kommersant.ru' + url
  whole_urls.append(whole_url)
print(whole_urls)

queries_list_of_lists = []
for url in urls:
  params2 = {
    "offset": 0,
    "limit": 100,
    "device_type_indicator": "ALL",
    "text_indicator": "QUERY",
    "filters": {
      "text_filters": [
        {
          "text_indicator": "URL",
          "operation": "TEXT_CONTAINS",
          "value": f"{url}"
        }
      ]
    }
  }
  query_monitor2 = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/query-analytics/list'

  r = requests.post(query_monitor2, headers=headers, json=params2)
  data2 = r.json()
  # print(json.dumps(data2, indent=4, ensure_ascii=False))
  queries = []
  for i in data2['text_indicator_to_statistics']:
    query_list = i['text_indicator']['value']
    queries.append(query_list)
  top_queries = queries[:6]
  queries_list_of_lists.append(top_queries)
# print(queries_list_of_lists)

dictionary = dict(zip(whole_urls, queries_list_of_lists))
print(dictionary)

# df = pd.DataFrame(whole_urls, queries_list_of_lists)
df = pd.DataFrame(list(dictionary.items ()), columns = ['URL', 'Запросы'])
df['Запросы'] = df['Запросы'].apply(lambda x: ', '.join(x))
print(df)
df = df.astype({'URL':str, 'Запросы':str})
df.to_csv('query_monitor.csv', index=False)

# Путь к локальному репозиторию
repo_path = '/seo_serv'

# Инициализация репозитория
repo = Repo(repo_path)

# Добавление файла query_monitor.csv
file_path = '/seo_serv/query_monitor.csv'
repo.index.add([file_path])

# Создание коммита
repo.index.commit('Добавлен файл query_monitor.csv')

# Отправка изменений на удаленный репозиторий
origin = repo.remote('origin')
origin.push()

# gc = gspread.service_account(filename='C:\\Users\\Alex\\OneDrive\\Рабочий стол\\Python\\dont touch\\key for bigquery gheets\\kommerssc-c8adc1a504f6.json')
# sh = gc.open("Мониторинг запросов API")
#
# worksheet_utm = sh.get_worksheet(0)
# worksheet_utm.update([df.columns.values.tolist()] + df.values.tolist())

# ----------- API поисковых запросов -------------------

# search_query = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/search-queries/popular?order_by=TOTAL_SHOWS&query_indicator=TOTAL_SHOWS&date_from={date1}&date_to={date2}&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION'

# пример хвоста:
# ?order_by=TOTAL_SHOWS
# &query_indicator=TOTAL_SHOWS
# &date_from={date1}&date_to={date2}
# &query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION
# &query_indicator=AVG_CLICK_POSITION'

# пример хвоста:
# ?text_indicator=URL
# &date_from={date1}&date_to={date2}
# &query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION
# &query_indicator=AVG_CLICK_POSITION'

# ?order_by=IMPRESSIONS
# &query_indicator=IMPRESSIONS
# &text_indicator=URL
# &limit=100
# &statistic_filters
# &date_from={date1}&date_to={date2}
# &query_indicator=TOTAL_CLICKS
# &query_indicator=AVG_SHOW_POSITION
# &query_indicator=AVG_CLICK_POSITION'