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
from yahoofinancials import YahooFinancials
from yahoo_fin import stock_info as si
import matplotlib.dates as mdates

google_news = GNews()
DATE_FORMAT = '%Y-%b-%d'

def google_scraper():
    start_date = datetime.strptime('2023-Oct-15', DATE_FORMAT)
    end_date = datetime.strptime('2023-Nov-15', DATE_FORMAT)
    worksheet = get_sheet().get_worksheet(0)
    nltk.download("vader_lexicon")
    #articles = news_search(start_date, end_date)
    #sentiment_analysis_of_articles = get_sentiment_analysis_of_articles(articles)
    #save_sentiment_analysis(worksheet, sentiment_analysis_of_articles)
    sentiment_analysis_of_articles = read_sentiment_analysis(worksheet)
    sentiment_analiisys_per_day = sentiment_analysis_of_articles.groupby('published date')['compound'].mean().reset_index().sort_values('published date')
    #print(sentiment_analiisys_per_day)
    print_stock_prices(sentiment_analiisys_per_day, start_date, end_date)
    print_revenue(start_date, end_date)  

def read_sentiment_analysis(worksheet):
    sentiment_analysis = worksheet.get_all_values()
    # Преобразование списка списков в датафрейм, где первый список - заголовки столбцов
    sentiment_analysis = pd.DataFrame(sentiment_analysis[1:], columns=sentiment_analysis[0])
    # Преобразование столбца 'published date' в datetime
    sentiment_analysis['published date'] = pd.to_datetime(sentiment_analysis['published date'])
    # Преобразование данных в столбце 'compound' в числа
    sentiment_analysis['compound'] = pd.to_numeric(sentiment_analysis['compound'].str.replace(',', '.'), errors='coerce')
    
    return sentiment_analysis

def print_revenue(start_date, end_date):
    ticker = 'NVDA'
    yahoo_financials = YahooFinancials(ticker)

    analysts_info = si.get_analysts_info(ticker)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)

    Revenue_Estimate = analysts_info['Revenue Estimate']
    print('-----Revenue Estimate-----')
    print(Revenue_Estimate)

    Earnings_History = analysts_info['Earnings History']
    print('-----Earnings History-----') 
    print(Earnings_History)


    income_statement = yahoo_financials.get_financial_stmts('quarterly', 'income')
    reports = income_statement['incomeStatementHistoryQuarterly'][ticker]

    data = []
    for report in reports:
        for date_str in report:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            if start_date.year == date.year and (start_date.month - 1) // 3 == (date.month - 1) // 3 and 'operatingRevenue' in report[date_str]:
                revenue_billion = report[date_str]['operatingRevenue'] / 1_000_000_000
                data.append((date, revenue_billion))

    # Создаем DataFrame из списка кортежей
    df = pd.DataFrame(data, columns=['Date', 'Revenue'])

    print('-----Revenue-----')
    print(df)

    # очистить значения на графике
    plt.cla()
    if not df.empty:
        plt.bar(df['Date'], df['Revenue'], color='green', width=20)
        # чтобы на графике были даты только за те данные которые есть
        plt.xticks([df['Date'].min()])
        plt.xlim(df['Date'].min() - pd.Timedelta(days=1), df['Date'].min() + pd.Timedelta(days=1))
        # Установка форматтера для оси X
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(DATE_FORMAT))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xlabel('Date')
    plt.ylabel('Revenue (in billions)')
    plt.title('Quarterly Revenue')
    plt.savefig('revenue.png')
    
def print_stock_prices(sentiment_data, start_date, end_date):
    # Получаем данные о ценах акций nvidia
    nvda = yf.download("NVDA", start_date, end_date, interval="1d")

    # Нарисовываем график цен акций nvidia
    plt.plot(nvda.index, nvda["Close"], label="NVDA")

    # Добавляем результаты анализа тональности на график
    for index, row in sentiment_data.iterrows():
        date = pd.to_datetime(row['published date'])
        if start_date <= date <= end_date:
            if date.strftime(DATE_FORMAT) in nvda.index:
                compound = float(row['compound'])
                color = 'g' if compound > 0.05 else 'r' if compound < -0.05 else 'b'
                plt.scatter(date, nvda.loc[date.strftime(DATE_FORMAT)]["Close"], c=color)
    # Устанавливаем интервал между метками на оси X
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(DATE_FORMAT))

    #plt.xticks(nvda.index)
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

def save_sentiment_analysis(worksheet, sentiment_analysis):
    sentiment_analysis['published date'] = sentiment_analysis['published date'].dt.strftime(DATE_FORMAT)
    # Создаем список, который содержит заголовки столбцов и данные
    data_with_headers = [sentiment_analysis.columns.values.tolist()] + sentiment_analysis.values.tolist()
    
    # Сохраняем данные в Google Sheets
    worksheet.update(data_with_headers)

def news_search(start_date, end_date):
    dates = [start_date + timedelta(days=i) for i in range((end_date-start_date).days + 1)]
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
