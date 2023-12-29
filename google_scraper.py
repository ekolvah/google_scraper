from bs4 import BeautifulSoup
import pandas as pd
import requests
from gnews import GNews
from newspaper import Article
import newspaper.article
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys

def google_scraper():
    nltk.download("vader_lexicon")
    keywords = ['nvidia', 'stock']
    date_from = '01/01/20233' 
    date_to = '02/01/20233'
    articles = news_search(keywords, date_from, date_to)
    result = sentiment_analysis(articles)
    print(result)

def news_search(keywords, date_from, date_to):
    google_news = GNews()
    for keyword in keywords:
      articles = google_news.get_news(keyword)
      #print(articles[0])
    return articles

def get_article_text(link):
   article = Article(link)
   article.download()
   article.parse()
   return article.text

def sentiment_analysis(articles):
    analyzer = SentimentIntensityAnalyzer()
    result = ''
    downloaded = 0 
    compound_sum = 0
    for article in articles:
      try:
        url = article['url']
        #print(url)
        article_text = get_article_text(url)
        #print(article_text)
        downloaded +=1
        scores = analyzer.polarity_scores(article_text)
        #print(scores)
        compound_sum += scores["compound"]
      except newspaper.article.ArticleException as e:
         pass
        
    compound_mean = compound_sum / len(articles)
    if compound_mean > 0.05:
        print("Тональность статей: положительная")
    elif compound_mean < -0.05:
        print("Тональность статей: отрицательная")
    else:
        print("Тональность статей: нейтральная")
    
    percentage = round(100*downloaded/len(articles))
    print("percentage of downloaded articles=" + str(percentage) + '%')
    
    return result

google_scraper()
