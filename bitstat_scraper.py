from gnews import GNews
import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
from bs4 import BeautifulSoup
import requests
import dateparser
import cryptocompare

google_news = GNews()
DATE_FORMAT = '%Y-%b-%d'
PLT_DATE_FORMAT = '%b-%d'
TICKET = 'BTC'
KEYWORDS = ['BTC'] 
START_DATE = datetime.strptime('2023-Dec-20', DATE_FORMAT)
END_DATE = datetime.now()

def bitstat_scraper():
    worksheet = get_sheet().get_worksheet(1)
    if os.getenv('RUN_IN_GITHUB_ACTION') == 'true':
        kit_actions = get_parsed_kit_actions()
    else:
        kit_actions = pd.DataFrame() 
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

    # Вычисляем смещение
    offset = hist_price_df["close"].min()*0.98

    for index, row in kit_actions_per_day.iterrows():
        date = pd.to_datetime(row.date)
        if START_DATE <= date <= END_DATE:
            if date.strftime(DATE_FORMAT) in hist_price_df.index:
                # уменьшаю высоту столбцов 
                diff_amount = float(row['diff_amount']) / offset 
                color = 'g' if diff_amount > 0 else 'r' 
                
                plt.scatter(date, hist_price_df.loc[date.strftime(DATE_FORMAT)]["close"], c=color)
                plt.bar(date, diff_amount, color=color, bottom=offset)
    # Устанавливаем интервал между метками на оси X
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(PLT_DATE_FORMAT))

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
    
def get_sheet():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = os.environ['CREDENTIALS']
    credentials_dict = json.loads(credentials)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1bravbHvEqZDg-5HgVOdH3367Ghs4XOfYNDuEF12LVbg/edit#gid=0')
    return sheet


bitstat_scraper()
