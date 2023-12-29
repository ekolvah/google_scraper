from bs4 import BeautifulSoup
import pandas as pd
import requests
from gnews import GNews
from newspaper import Article

def google_scraper():
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
    result = ''
    downloaded = 0 
    do_not_downloaded = 0
    for article in articles:
      try:
        get_article_text(article['url'])
        #print('Article downloaded from ' + article['url'])
        downloaded +=1
      except:
        #print('Article DO NOT downloaded from ' + article['url'])
        do_not_downloaded +=1
    percentage = 100*downloaded/(downloaded+do_not_downloaded)
    print('downloaded ' + str(downloaded))
    print('do not downloaded ' + str(do_not_downloaded))
    print("percentage of downloaded articles=" + str(percentage) + '%')
    return result

google_scraper()
