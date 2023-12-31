from gnews import GNews
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import datetime, timedelta
import matplotlib.dates as mdates
from bs4 import BeautifulSoup
import requests


google_news = GNews()
DATE_FORMAT = '%Y-%b-%d'
TICKET = 'BTC'
KEYWORDS = ['BTC'] 
START_DATE = datetime.strptime('2023-Dec-20', DATE_FORMAT)
END_DATE = datetime.now()

def google_scraper():
    worksheet = get_sheet().get_worksheet(0)
    nltk.download("vader_lexicon")
    articles = news_search()
    sentiment_analysis_of_articles = get_sentiment_analysis_of_articles(articles)
    save_sentiment_analysis(worksheet, sentiment_analysis_of_articles)
    sentiment_analysis_of_articles = read_sentiment_analysis(worksheet)
    sentiment_analiisys_per_day = sentiment_analysis_of_articles.groupby('published date')['compound'].mean().reset_index().sort_values('published date')
    print(sentiment_analiisys_per_day)
    print_stock_prices(sentiment_analiisys_per_day)

def get_soup(URL):
  headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
      'Content-Type': 'text/html',
  }

  response = requests.get(URL, headers=headers)
  soup = BeautifulSoup(response.text, 'html.parser')
  if response.status_code != 200:
      print("******** fail ********** ")
  #print(response.url)
  #print(response.text)
  #print('---')
  return soup
    
def read_sentiment_analysis(worksheet):
    sentiment_analysis = worksheet.get_all_values()
    # Преобразование списка списков в датафрейм, где первый список - заголовки столбцов
    sentiment_analysis = pd.DataFrame(sentiment_analysis[1:], columns=sentiment_analysis[0])
    # Преобразование столбца 'published date' в datetime
    sentiment_analysis['published date'] = pd.to_datetime(sentiment_analysis['published date'])
    # Преобразование данных в столбце 'compound' в числа
    sentiment_analysis['compound'] = pd.to_numeric(sentiment_analysis['compound'].str.replace(',', '.'), errors='coerce')
    
    return sentiment_analysis

    
def print_stock_prices(sentiment_data):
    # Получаем данные о ценах акций 
    tckt = yf.download(TICKET, START_DATE, END_DATE, interval="1d")

    # Нарисовываем график цен акций 
    plt.plot(tckt.index, tckt["Close"], label=TICKET)

    # Добавляем результаты анализа тональности на график
    for index, row in sentiment_data.iterrows():
        date = pd.to_datetime(row['published date'])
        if START_DATE <= date <= END_DATE:
            if date.strftime(DATE_FORMAT) in tckt.index:
                compound = float(row['compound'])
                color = 'g' if compound > 0.05 else 'r' if compound < -0.05 else 'b'
                plt.scatter(date, tckt.loc[date.strftime(DATE_FORMAT)]["Close"], c=color)
    # Устанавливаем интервал между метками на оси X
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(DATE_FORMAT))

    #plt.xticks(nvda.index)
    plt.title("Цены акций " + TICKET + " и результаты анализа тональности новостей")
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

def save_sentiment_analysis(worksheet, sentiment_analysis):
    sentiment_analysis['published date'] = sentiment_analysis['published date'].dt.strftime(DATE_FORMAT)
    # Создаем список, который содержит заголовки столбцов и данные
    data_with_headers = [sentiment_analysis.columns.values.tolist()] + sentiment_analysis.values.tolist()
    
    # Сохраняем данные в Google Sheets
    worksheet.update(data_with_headers)

def news_search():
    # Получение списка дат в формате (год, месяц, день) так чтобы между датами был интервал в 2 дня
    dates = [START_DATE + timedelta(days=i) for i in range((END_DATE-START_DATE).days + 1)]
    dates = [(date.year, date.month, date.day) for date in dates]  

    articles = []
    for i in range(len(dates) - 2):
        start_date = dates[i]
        end_date = dates[i + 2]
        print(start_date, end_date)
        articles += news_search_for_dates(start_date, end_date, KEYWORDS)
    
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
            # Удаление информации о времени, сохранение только даты
            date_time_obj = date_time_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            data.append([date_time_obj, str(article['publisher']), str(article['title']), float(scores["compound"])])
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

