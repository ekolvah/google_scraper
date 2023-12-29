from gnews import GNews
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys

google_news = GNews()

def google_scraper():
    nltk.download("vader_lexicon")
    keywords = ['nvidia stock articles']
    date_from = '01/01/20233' 
    date_to = '02/01/20233'
    articles = news_search(keywords, date_from, date_to)
    result = sentiment_analysis(articles)
    print(result)

def news_search(keywords, date_from, date_to):
    google_news.period = '1d'
    for keyword in keywords:
      articles = google_news.get_news(keyword)
    return articles

def sentiment_analysis(articles):
    analyzer = SentimentIntensityAnalyzer()
    result = ''
    downloaded = 0 
    compound_sum = 0
    for article in articles:
        url = article['url']
        #print(url)
        article_obj = google_news.get_full_article(url)
        if article_obj is not None:
            article_text = google_news.get_full_article(url).text
            print(str(article['published date']) + " " + str(article['publisher']) + " " + str(article['title']))
            downloaded +=1
            scores = analyzer.polarity_scores(article_text)
            #print(scores)
            compound_sum += scores["compound"]
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
    
    return result

google_scraper()
