import pandas as pd
from pytrends.request import TrendReq
import schedule
import time
from datetime import datetime, timedelta
from git import Repo
import csv
from github import Github

def main():
    try:
        # Инициализация pytrends API
        pytrend = TrendReq(hl='ru', tz=180, timeout=(10, 25), retries=2, backoff_factor=0.1)
        # requests_args = {'verify': False}

        # Настройка параметров для запроса
        keyword = ''  # Используем пустую строку, если мы хотим общие темы для страны, а не для конкретного поискового запроса
        timeframe = 'today 1-m'  # За последний месяц
        geo = 'RU'  # Геолокация - Россия
        category = 47  # Категория

        # Строим запрос для получения связанных тем
        pytrend.build_payload(kw_list=[keyword], timeframe=timeframe, geo=geo, cat=category)

        # # Получаем связанные темы
        # related_topics = pytrend.related_topics()
        related_topics = pytrend.related_topics()
        df_topics = pd.DataFrame(related_topics.get(keyword).get('rising'))
        df_topics = df_topics[['topic_title', 'value']]
        df_topics = df_topics.rename(columns={'topic_title': 'name'})

        # Получаем связанные темы
        related_queries = pytrend.related_queries()
        df_queries = pd.DataFrame(related_queries.get(keyword).get('rising'))
        df_queries = df_queries[['query', 'value']]
        df_queries = df_queries.rename(columns={'query': 'name'})

        df_all = pd.concat([df_topics, df_queries])
        df_all.sort_values('value', ascending=False, inplace=True)
        df_all.to_csv('autopilot_trends.csv', index=False)


        # Путь к локальному репозиторию
        repo_path = '/seo_serv'

        # Инициализация репозитория
        repo = Repo(repo_path)

        # Добавление файла today_trends.csv
        file_path = '/seo_serv/autopilot_trends.csv'
        repo.index.add([file_path])

        # Создание коммита
        repo.index.commit('Добавлен файл autopilot_trends.csv')

        # Отправка изменений на удаленный репозиторий
        origin = repo.remote('origin')
        origin.push()
        print('готово')
    except Exception as e:
        print(f'Похоже гугл снова ругается на доступ к api Google Trends: {e}')

# настройка расписания
schedule.every().day.at("08:00", "Europe/Moscow").do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()

