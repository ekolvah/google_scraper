from gnews import GNews
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

google_news = GNews()

def google_scraper():
    worksheet = get_sheet().get_worksheet(0)
    nltk.download("vader_lexicon")
    keywords = ['nvidia stock articles']
    date_from = '01/01/20233' 
    date_to = '02/01/20233'
    articles = news_search(keywords, date_from, date_to)
    result_of_analysis = sentiment_analysis(articles)
    save_sentiment_analysis(worksheet, result_of_analysis)

def get_sheet():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = os.environ['CREDENTIALS']
    credentials_dict = json.loads(credentials)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1bravbHvEqZDg-5HgVOdH3367Ghs4XOfYNDuEF12LVbg/edit#gid=0')
    return sheet

def save_sentiment_analysis(worksheet, result_of_analysis):
    worksheet.update(values=result_of_analysis.values.tolist(), range_name=None)

def news_search(keywords, date_from, date_to):
    google_news.period = '1d'
    for keyword in keywords:
      articles = google_news.get_news(keyword)
    return articles

def sentiment_analysis(articles):
    data = []
    analyzer = SentimentIntensityAnalyzer()
    downloaded = 0 
    compound_sum = 0
    for article in articles:
        url = article['url']
        #print(url)
        article_obj = google_news.get_full_article(url)
        if article_obj is not None:
            article_text = google_news.get_full_article(url).text
            downloaded +=1
            scores = analyzer.polarity_scores(article_text)
            #print(scores)
            compound_sum += scores["compound"]
            data.append([str(article['published date']), str(article['publisher']), str(article['title'])])
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
    
    result_of_analysis = pd.DataFrame(data, columns=['published date', 'publisher', 'title'])
    return result_of_analysis

google_scraper()
