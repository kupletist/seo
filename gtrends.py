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
        # pytrend = TrendReq(hl='ru', tz=180, timeout=(10,25), retries=2, backoff_factor=0.1, requests_args={'verify':False})
        pytrend = TrendReq(hl='ru', tz=180, timeout=(10,25), retries=2, backoff_factor=0.1, requests_args={'verify':True})

    
        # realtime
        today_trends = pytrend.trending_searches(pn='russia')
    
        today_trends.to_csv('today_trends.csv', index=False)
    
        realtime = pytrend.realtime_trending_searches(pn='RU')
        realtime_df = realtime['title']
        realtime_df.to_csv('realtime_trends.csv', index=False)
        # Путь к локальному репозиторию
        repo_path = '/seo_serv'
    
        # Инициализация репозитория
        repo = Repo(repo_path)
    
        # Добавление файла today_trends.csv
        file_path = '/seo_serv/today_trends.csv'
        repo.index.add([file_path])
    
        file_path = '/seo_serv/realtime_trends.csv'
        repo.index.add([file_path])
    
        # Создание коммита
        repo.index.commit('Добавлены файлы today_trends.csv / realtime_trends.csv')
    
        # Отправка изменений на удаленный репозиторий
        origin = repo.remote('origin')
        origin.push()
        print('готово')
    except Exception:
        print(f'Похоже гугл снова ругается на доступ к api Google Trends')
    

# настройка расписания
schedule.every(2).hours.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()

