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
import dateparser
import cryptocompare

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

def bitstat_parcing():
    worksheet = get_sheet().get_worksheet(1)
    kit_actions = get_parsed_kit_actions()
    saved_kit_actions = get_saved_kit_actions(worksheet)
    # добавляем saved_kit_actions к kit_actions
    kit_actions = pd.concat([kit_actions, saved_kit_actions])
    # удаляем дубликаты
    kit_actions = kit_actions.drop_duplicates()
    save_kit_actions(worksheet, kit_actions)
    kit_actions_per_day = get_kit_actions_per_day(kit_actions)
    print_kit_actions(kit_actions_per_day)

# группировка по дням и вычисление суммы значения diff_amount за день
def get_kit_actions_per_day(kit_actions):
    kit_actions['date'] = pd.to_datetime(kit_actions['date'])
    kit_actions_per_day = kit_actions.groupby('date')['diff_amount'].sum().reset_index().sort_values('date')
    print('-----kit_actions_per_day-----')
    print(kit_actions_per_day)
    
    return kit_actions_per_day

def print_kit_actions(kit_actions_per_day):
    # Вычисляем количество дней между START_DATE и END_DATE
    days = (END_DATE - START_DATE).days

    # Получаем исторические данные
    hist_price = cryptocompare.get_historical_price_day(TICKET, currency='USD', limit=days, toTs=END_DATE)
    
    # Преобразуем данные в DataFrame
    hist_price_df = pd.DataFrame(hist_price)
    # Устанавливаем дату в качестве индекса
    hist_price_df['time'] = pd.to_datetime(hist_price_df['time'], unit='s')
    hist_price_df.set_index('time', inplace=True, drop=False)

    print('-----hist_price_df-----')
    print(hist_price_df)

    plt.plot(hist_price_df.index, hist_price_df["close"], label=TICKET)

    # Добавляем результаты анализа тональности на график
    for index, row in kit_actions_per_day.iterrows():
        date = pd.to_datetime(row.date)
        if START_DATE <= date <= END_DATE:
            if date.strftime(DATE_FORMAT) in hist_price_df.index:
                compound = float(row['diff_amount'])
                color = 'g' if compound > 0 else 'r' 
                plt.scatter(date, hist_price_df.loc[date.strftime(DATE_FORMAT)]["close"], c=color)
    # Устанавливаем интервал между метками на оси X
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(DATE_FORMAT))

    #plt.xticks(nvda.index)
    plt.title("Цены акций " + TICKET )
    plt.xlabel("Дата")
    plt.ylabel("Цена")
    plt.legend()

    # Сохраняем график в файл
    plt.savefig('BTC_prices.png')

def get_saved_kit_actions(worksheet):
    kit_actions = worksheet.get_all_values()
    if len(kit_actions) > 1:
        # Преобразование списка списков в датафрейм, где первый список - заголовки столбцов
        kit_actions = pd.DataFrame(kit_actions[1:], columns=kit_actions[0])
        # Преобразование данных в столбце 'compound' в числа
        kit_actions['date_time'] = pd.to_datetime(kit_actions['date_time'])
        kit_actions['amount'] = pd.to_numeric(kit_actions['amount'].str.replace(',', '.'), errors='coerce')
        kit_actions['amount_usd'] = pd.to_numeric(kit_actions['amount_usd'].str.replace(',', '.'), errors='coerce')
        kit_actions['btc_rate'] = pd.to_numeric(kit_actions['btc_rate'].str.replace(',', '.'), errors='coerce')
        kit_actions['btc_transaction_rate'] = pd.to_numeric(kit_actions['btc_transaction_rate'].str.replace(',', '.'), errors='coerce')
        kit_actions['diff_amount'] = pd.to_numeric(kit_actions['diff_amount'].str.replace(',', '.'), errors='coerce')
    else:
        kit_actions = pd.DataFrame()
    
    return kit_actions
def save_kit_actions(worksheet, kit_actions):
    # сохраняем данные в формате str потому что в противном случае возникает ошибка при сохранении в Google Sheets TimeStamp
    kit_actions = kit_actions.astype(str)
    # Создаем список, который содержит заголовки столбцов и данные
    data_with_headers = [kit_actions.columns.values.tolist()] + kit_actions.values.tolist()
    # Сохраняем данные в Google Sheets
    worksheet.update(data_with_headers)

# парсинг ОТСЛЕЖИВАНИЕ ДЕЙСТВИЙ КИТОВ BTC из bitstat.top 
def get_parsed_kit_actions():
    num_pages = 50
    base_url = 'https://bitstat.top/whales_transactions.php?page={}&t=btc&l=0'
    urls = [base_url.format(i) for i in range(1, num_pages + 1)]

    data = []
    for url in urls:
        soup = get_soup(url)
        print(url)

        for div in soup.select('.cr'):
            amount = div.select_one('.trx-amount')
            amount_usd = div.select_one('.trx-amount_usd')
            ch_btc = div.select_one('.ch_btc span.grey_font.small-font')
            
            date_element = div.select_one('.trx-date span')
            date_html = str(date_element).replace('<br/>', ' ')
            date_str = BeautifulSoup(date_html, 'html.parser').get_text()

            # форматирование строк в данные 
            amount_value = float(amount.text.replace(' ', ''))
            amount_usd_value = float(amount_usd.text.replace('$', '').replace(' ', ''))
            btc_rate_value = round(float(ch_btc.text.replace(' ', '')))
            btc_transaction_rate = round(amount_usd_value / amount_value)
            diff_amount = round((btc_rate_value - btc_transaction_rate)*amount_value)
            
            date_time = dateparser.parse(date_str)
            date = date_time.replace(hour=0, minute=0, second=0, microsecond=0)

            data.append([date_time,
                         amount_value, 
                         amount_usd_value, 
                         date, 
                         btc_rate_value, 
                         btc_transaction_rate,
                         diff_amount])

    kit_actions = pd.DataFrame(data, columns=['date_time',
                                              'amount', 
                                              'amount_usd', 
                                              'date', 
                                              'btc_rate', 
                                              'btc_transaction_rate', 
                                              'diff_amount'])

    return kit_actions
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

#google_scraper()
bitstat_parcing()
