  def google_scraper():
    keywords = ['nvidia', 'stock']
    date_from = '01/01/20233' 
    date_to = '02/01/20233'
    articles = news_search(keywords, date_from, date_to)
    result = sentiment_analysis(articles)
    print(result)

  def news_search(keywords, date_from, date_to):
    result = ['text']
    return result

  def sentiment_analysis(articles):
    result = ''
    for article in articles:
      result += ' ' + article
    return result

google_scraper()
