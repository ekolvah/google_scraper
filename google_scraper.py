from gnews import GNews
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import datetime, timedelta

google_news = GNews()
DAYS = 30

def google_scraper():
    worksheet = get_sheet().get_worksheet(0)
    nltk.download("vader_lexicon")
    articles = news_search()
    sentiment_analysis_of_articles = get_sentiment_analysis_of_articles(articles)
    save_sentiment_analysis(worksheet, sentiment_analysis_of_articles)
    #sentiment_analysis_of_articles = read_sentiment_analysis(worksheet)
    sentiment_analiisys_per_day = sentiment_analysis_of_articles.groupby('published date')['compound'].mean().reset_index()
    print(sentiment_analiisys_per_day)
    print_stock(sentiment_analiisys_per_day)

def read_sentiment_analysis(worksheet):
    sentiment_analysis = worksheet.get_all_values()
    sentiment_analysis = pd.DataFrame(sentiment_analysis[1:], columns=sentiment_analysis[0])
    
    # Преобразование данных в столбце 'compound' в числа
    sentiment_analysis['compound'] = pd.to_numeric(sentiment_analysis['compound'].str.replace(',', '.'), errors='coerce')
    
    return sentiment_analysis

def print_stock(sentiment_data):
    # Задаем начальную и конечную дату
    end = datetime.now().date()
    start = end - timedelta(DAYS)
    
    # Получаем данные о ценах акций nvidia
    nvda = yf.download("NVDA", start, end, interval="1d")

    print(nvda)
    # Нарисовываем график цен акций nvidia
    plt.plot(nvda.index, nvda["Close"], label="NVDA")

    # Добавляем результаты анализа тональности на график
    for index, row in sentiment_data.iterrows():
        date = pd.to_datetime(row['published date']).date()
        if start <= date <= end:
            if date.strftime('%Y-%m-%d') in nvda.index:
                compound = float(row['compound'])
                color = 'g' if compound > 0.05 else 'r' if compound < -0.05 else 'b'
                plt.scatter(date, nvda.loc[date.strftime('%Y-%m-%d')]["Close"], c=color)
    plt.title("Цены акций nvidia за 2021 год")
    plt.xlabel("Дата")
    plt.ylabel("Цена")
    plt.legend()

    # Сохраняем график в файл
    plt.savefig('stock_prices.png')

def get_sheet():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = os.environ['CREDENTIALS']
    credentials_dict = json.loads(credentials)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1bravbHvEqZDg-5HgVOdH3367Ghs4XOfYNDuEF12LVbg/edit#gid=0')
    return sheet

def save_sentiment_analysis(worksheet, result_of_analysis):
    # Создаем список, который содержит заголовки столбцов и данные
    data_with_headers = [result_of_analysis.columns.values.tolist()] + result_of_analysis.values.tolist()
    
    # Сохраняем данные в Google Sheets
    worksheet.update(data_with_headers)

def news_search():
    # Получаем список дат за последние DAYS дней для того чтобы сделать поиск новостей за эти дни в цикле, потому что API Google News позволяет получить только 100 статей за один запрос
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(DAYS)
    dates = [start + timedelta(days=i) for i in range((end-start).days + 1)]
    dates = [(date.year, date.month, date.day) for date in dates]  

    keywords = ['nvidia stock articles']

    articles = []
    for i in range(len(dates) - 2):
        start_date = dates[i]
        end_date = dates[i + 2]
        print(start_date, end_date)
        articles += news_search_for_dates(start_date, end_date, keywords)
    
    # Удаление дубликатов
    articles = [json.loads(article) for article in set(json.dumps(article) for article in articles)]

    return articles

def news_search_for_dates(start_date, end_date, keywords):
    google_news.start_date = start_date
    google_news.end_date = end_date
    articles = []
    for keyword in keywords:
      articles += google_news.get_news(keyword)
    return articles

def get_sentiment_analysis_of_articles(articles):
    data = []
    analyzer = SentimentIntensityAnalyzer()
    downloaded = 0 
    compound_sum = 0
    for article in articles:
        url = article['url']
        article_obj = google_news.get_full_article(url)
        if article_obj is not None:
            article_text = article_obj.text
            downloaded +=1
            scores = analyzer.polarity_scores(article_text)
            compound_sum += scores["compound"]
            # Преобразование строки даты и времени в объект datetime
            date_time_obj = datetime.strptime(article['published date'], '%a, %d %b %Y %H:%M:%S %Z')
            # Форматирование объекта datetime обратно в строку, содержащую только дату
            date_str = date_time_obj.strftime('%a, %d %b %Y')
            
            data.append([date_str, str(article['publisher']), str(article['title']), float(scores["compound"])])
        else:   
            continue
    print('Количество статей обработано ' + str(len(articles)))
    compound_mean = compound_sum / len(articles)
    print('Тональность статей: ' + str(compound_mean))
    if compound_mean > 0.05:
        print("Тональность статей: положительная")
    elif compound_mean < -0.05:
        print("Тональность статей: отрицательная")
    else:
        print("Тональность статей: нейтральная")
    
    percentage = round(100*downloaded/len(articles))
    print("percentage of downloaded articles=" + str(percentage) + '%')
    
    sentiment_analysis_of_articles = pd.DataFrame(data, columns=['published date', 'publisher', 'title', 'compound'])
    return sentiment_analysis_of_articles

google_scraper()
